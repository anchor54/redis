package geohash

import "fmt"

const LatMin = -85.05112878
const LatMax = 85.05112878
const LonMin = -180
const LonMax = 180

func normalize(x float64, min float64, max float64) (int, error) {
	if x < min || x > max {
		return -1, fmt.Errorf("%f should lie between [%f, %f]", x, min, max)
	}
	_range := max - min
	return int(((x - min) / _range) * (2 ^ 26)), nil
}

func interleave(x int, y int) int {
    // First, the values are spread from 32-bit to 64-bit integers.
    // This is done by inserting 32 zero bits in-between.
    //
    // Before spread: x1  x2  ...  x31  x32
    // After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
    x = spreadInt32ToInt64(x)
    y = spreadInt32ToInt64(y)

    // The y value is then shifted 1 bit to the left.
    // Before shift: 0   y1   0   y2 ... 0   y31   0   y32
    // After shift:  y1   0   y2 ... 0   y31   0   y32   0
    yShifted := y << 1

    // Next, x and y_shifted are combined using a bitwise OR.
    //
    // Before bitwise OR (x): 0   x1   0   x2   ...  0   x31    0   x32
    // Before bitwise OR (y): y1  0    y2  0    ...  y31  0    y32   0
    // After bitwise OR     : y1  x2   y2  x2   ...  y31  x31  y32  x32
    return x | yShifted
}

// Spreads a 32-bit integer to a 64-bit integer by inserting
// 32 zero bits in-between.
//
// Before spread: x1  x2  ...  x31  x32
// After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
func spreadInt32ToInt64(v int) int {
    // Ensure only lower 32 bits are non-zero.
    v = v & 0xFFFFFFFF

    // Bitwise operations to spread 32 bits into 64 bits with zeros in-between
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8))  & 0x00FF00FF00FF00FF
    v = (v | (v << 4))  & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2))  & 0x3333333333333333
    v = (v | (v << 1))  & 0x5555555555555555

    return v
}

func GetCoordinateScore(lat float64, lon float64) (int, error) {
	normalizedLat, err := normalize(lat, float64(LatMin), float64(LatMax))
	if err != nil {
		return -1, err
	}
	normalizedLon, err := normalize(lon, LonMin, LonMax)
	if err != nil {
		return -1, err
	}
	return interleave(normalizedLat, normalizedLon), nil
}
