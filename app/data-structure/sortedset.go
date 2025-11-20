package datastructure

import (
	"cmp"

	skiplist "github.com/anchor54/SkipList"
)

type KeyValue struct {
	Key string
	Value float64
}

func (kv KeyValue) Compare(other KeyValue) int {
	if kv.Value == other.Value {
		return cmp.Compare(kv.Key, other.Key)
	}
	return cmp.Compare(kv.Value, other.Value)
}

type SortedSet struct {
	skipList	*skiplist.SkipList[KeyValue]
	score		map[string]float64
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		skipList:	skiplist.NewComparableSkipList[KeyValue](),
		score:		make(map[string]float64),
	}
}

func (ss SortedSet) Kind() RType {
	return RSortedSet
}

func AsSortedSet(v RValue) (*SortedSet, bool) {
	if sv, ok := v.(*SortedSet); ok {
		return sv, true
	}
	return nil, false
}

func (ss *SortedSet) Add(args []KeyValue) int {
	count := 0
	for _, keyValue := range args {
		if _, ok := ss.score[keyValue.Key]; ok {
			continue
		}
		ss.score[keyValue.Key] = keyValue.Value
		ss.skipList.Add(keyValue)
		count++
	}
	return count
}

func (ss *SortedSet) GetRank(key string) int {
	score, ok := ss.score[key]
	if !ok {
		return -1
	}
	rank, _ := ss.skipList.GetRank(KeyValue{Key: key, Value: score})
	return rank - 1
}