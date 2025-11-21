package geohash

import "fmt"

const LatMin float64 = -85.05112878
const LatMax float64 = 85.05112878
const LonMin float64 = -180
const LonMax float64 = 180
const LatRange float64 = LatMax - LatMin
const LonRange float64 = LonMax - LonMin

func normalize(x float64, min float64, max float64) (int, error) {
	if x < min || x > max {
		return -1, fmt.Errorf("%f should lie between [%f, %f]", x, min, max)
	}
	_range := max - min
	return int(((x - min) / _range) * (1 << 26)), nil
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

func compactInt64ToInt32(v int64) int {
    // Keep only the bits in even positions
    v = v & 0x5555555555555555

    // Before masking: w1   v1  ...   w2   v16  ... w31  v31  w32  v32
    // After masking: 0   v1  ...   0   v16  ... 0  v31  0  v32

    // Where w1, w2,..w31 are the digits from longitude if we're compacting latitude, or digits from latitude if we're compacting longitude
    // So, we mask them out and only keep the relevant bits that we wish to compact

    // ------
    // Reverse the spreading process by shifting and masking
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF

    // Before compacting: 0   v1  ...   0   v16  ... 0  v31  0  v32
    // After compacting: v1  v2  ...  v31  v32
    // -----
    
    return int(v)
}

func convertGridNumbersToCoordinates(gridLat int, gridLon int) (float64, float64) {
    // Calculate the grid boundaries
    gridLatMin := LatMin + LatRange * (float64(gridLat) / (1 << 26))
    gridLatMax := LatMin + LatRange * (float64(gridLat + 1) / (1 << 26))
    gridLongMin := LonMin + LonRange * (float64(gridLon) / (1 << 26))
    gridLongMax := LonMin + LonRange * (float64(gridLon + 1) / (1 << 26))
    
    // Calculate the center point of the grid cell
    latitude := (gridLatMin + gridLatMax) / 2
    longitude := (gridLongMin + gridLongMax) / 2
    return latitude, longitude
}

func EncodeCoordinates(lat float64, lon float64) (int, error) {
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

func DecodeCoordinates(geoHash int64) (float64, float64) {
    gridLatInt := compactInt64ToInt32(geoHash)
    gridLonInt := compactInt64ToInt32(geoHash >> 1)
    return convertGridNumbersToCoordinates(gridLatInt, gridLonInt)
}
