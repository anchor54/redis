package core

import (
	"errors"
	"math"
	"strconv"
	"time"

	ds "github.com/codecrafters-io/redis-starter-go/app/data-structure"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

var db = KVStore{store: ds.SyncMap[string, RedisObject]{}}

var INVALID_ARGUMENTS_ERROR = "Invalid argumetns"
var OK_RESPONSE = "OK"
var SOMETHING_WENT_WRONG = "Something went wrong"

func pingHandler(args ...string) (string, error) {
	return utils.ToSimpleString("PONG"), nil
}

func echoHandler(args ...string) (string, error) {
	if len(args) == 0 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	} else {
		return utils.ToBulkString(args[0]), nil
	}
}

func setHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	} else {
		key, value := args[0], args[1]
		kv_obj := NewStringObject(value)
		db.Store(key, &kv_obj)

		// check if expiry is set
		if len(args) >= 4 {
			exp_time, err := strconv.Atoi(args[3])

			if err != nil {
				return "", err
			}

			switch exp_type := args[2]; exp_type {
			case "EX":
				kv_obj.SetExpiry(time.Duration(exp_time) * time.Second)
			case "PX":
				kv_obj.SetExpiry(time.Duration(exp_time) * time.Millisecond)
			}
		}

		return utils.ToSimpleString(OK_RESPONSE), nil
	}
}

func getHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}
	val, ok := db.GetString(args[0])
	if !ok {
		return utils.ToNullBulkString(), nil
	}
	return utils.ToBulkString(val), nil
}

func lpushHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key, value := args[0], args[1:]
	utils.Reverse(value)
	dq, _ := db.LoadOrStoreList(key)

	if dq == nil {
		return "", errors.New("failed to load or store list")
	}

	n := dq.PushFront(value...)
	return utils.ToRespInt(n), nil
}

func rpushHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key, value := args[0], args[1:]
	dq, _ := db.LoadOrStoreList(key)

	if dq == nil {
		return "", errors.New("failed to load or store list")
	}

	n := dq.PushBack(value...)
	return utils.ToRespInt(n), nil
}

func lrangeHandler(args ...string) (string, error) {
	if len(args) < 3 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}
	key := args[0]
	l, err := strconv.Atoi(args[1])

	if err != nil {
		return "", err
	}

	r, err := strconv.Atoi(args[2])

	if err != nil {
		return "", err
	}

	list, ok := db.GetList(key)

	if !ok {
		return utils.ToArray(make([]string, 0)), nil
	}

	return utils.ToArray(list.LRange(l, r)), nil
}

func llenHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]
	list, ok := db.GetList(key)
	if !ok {
		return utils.ToRespInt(0), nil
	}

	return utils.ToRespInt(list.Length()), nil
}

func lpopHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]
	dq, ok := db.GetList(key)
	if !ok {
		return utils.ToNullBulkString(), nil
	}

	if len(args) > 1 {
		n_ele_to_rem, _ := strconv.Atoi(args[1])
		removed_elements, n_removed_elements := dq.TryPopN(n_ele_to_rem)
		if n_removed_elements == 0 {
			return utils.ToNullBulkString(), nil
		}
		return utils.ToArray(removed_elements), nil
	}

	removed_element, ok := dq.TryPop()
	if !ok {
		return utils.ToNullBulkString(), nil
	}
	return utils.ToBulkString(removed_element), nil
}

func blpopHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]
	timeout, _ := strconv.ParseFloat(args[1], 32)
	dq, _ := db.LoadOrStoreList(key)

	if dq == nil {
		return "", errors.New("failed to load or store list")
	}

	val, ok := dq.PopFront(timeout)
	if !ok {
		return utils.ToArray(nil), nil
	}

	return utils.ToArray([]string{key, val}), nil
}

func typeHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]

	val, ok := db.GetValue(key)
	if !ok {
		return utils.ToSimpleString("none"), nil
	}

	switch val.Get().Kind() {
	case ds.RString:
		return utils.ToSimpleString("string"), nil
	case ds.RList:
		return utils.ToSimpleString("list"), nil
	case ds.RStream:
		return utils.ToSimpleString("stream"), nil
	default:
		return utils.ToSimpleString("none"), nil
	}
}

func xaddHandler(args ...string) (string, error) {
	if len(args) < 4 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key, entry_id := args[0], args[1]
	fields := args[2:]

	stream, _ := db.LoadOrStoreStream(key)
	entry, err := stream.CreateEntry(entry_id, fields...)
	if err != nil {
		return "", err
	}
	stream.AppendEntry(entry)
	return utils.ToBulkString(entry.ID.String()), nil
}

func xrangeHandler(args ...string) (string, error) {
	if len(args) < 3 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]
	startID, err := ds.ParseStartStreamID(args[1])
	if err != nil {
		return "", err
	}
	endID, err := ds.ParseEndStreamID(args[2])
	if err != nil {
		return "", err
	}

	stream, _ := db.LoadOrStoreStream(key)
	entries, err := stream.RangeScan(startID, endID)
	if err != nil {
		return "", err
	}
	return utils.ToStreamEntries(entries), nil
}

func xreadHandler(args ...string) (string, error) {
	if len(args) < 3 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	args = args[1:]
	n := len(args)
	if n%2 != 0 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	streamsKeys := args[0 : n/2]
	startIDs := args[n/2:]

	streams := make([]utils.StreamKeyEntries, 0)
	for i := 0; i < n/2; i++ {
		stream, _ := db.LoadOrStoreStream(streamsKeys[i])
		startID, err := ds.ParseStartStreamID(startIDs[i])
		if err != nil {
			return "", err
		}
		startID.Seq++
		entries, err := stream.RangeScan(startID, ds.StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64})
		if err != nil {
			return "", err
		}
		streams = append(streams, utils.StreamKeyEntries{Key: streamsKeys[i], Entries: entries})
	}

	return utils.ToStreamEntriesByKey(streams), nil
}

var Handlers = map[string]func(...string) (string, error){
	"PING":   pingHandler,
	"ECHO":   echoHandler,
	"SET":    setHandler,
	"GET":    getHandler,
	"LPUSH":  lpushHandler,
	"RPUSH":  rpushHandler,
	"LRANGE": lrangeHandler,
	"LLEN":   llenHandler,
	"LPOP":   lpopHandler,
	"BLPOP":  blpopHandler,
	"TYPE":   typeHandler,
	"XADD":   xaddHandler,
	"XRANGE": xrangeHandler,
	"XREAD":  xreadHandler,
}
