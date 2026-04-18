package lock

import (
	"hash/fnv"
	"sort"
	"sync"
)

type Locks struct {
	table []*sync.RWMutex
}

func Make(size int) *Locks {
	table := make([]*sync.RWMutex, size)
	for i := range table {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table: table,
	}

}
func (lock *Locks) spread(key string) uint32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return hash.Sum32() % uint32(len(lock.table))
}
func (lock *Locks) Lock(key string) {
	index := lock.spread(key)
	lock.table[index].Lock()
}
func (locks *Locks) UnLock(key string) {
	index := locks.spread(key)
	locks.table[index].Unlock()
}
func (locks *Locks) Locks(keys ...string) {
	indices := locks.toLockIndices(keys)
	for _, index := range indices {
		locks.table[index].Lock()
	}

}
func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys)
	for i := len(indices) - 1; i >= 0; i-- {
		locks.table[indices[i]].Unlock()
	}
}
func (lock *Locks) toLockIndices(keys []string) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		indexMap[lock.spread(key)] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] < indices[j]
	})
	return indices
}
func (lock *Locks) RLock(key string) {
	index := lock.spread(key)
	lock.table[index].RLock()
}
func (lock *Locks) RUnlock(key string) {
	index := lock.spread(key)
	lock.table[index].RUnlock()
}
func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys)
	for _, index := range indices {
		locks.table[index].RLock()
	}
}
func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys)
	for i := len(indices) - 1; i >= 0; i-- {
		locks.table[indices[i]].RUnlock()
	}
}
func (lock *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := lock.toLockIndices(keys)
	writeIndexSet := make(map[uint32]struct{})
	for _, key := range writeKeys {
		index := lock.spread(key)
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		if _, ok := writeIndexSet[index]; ok {
			lock.table[index].Lock()
		} else {
			lock.table[index].RLock()
		}
	}

}
func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys)

	writeIndexSet := make(map[uint32]struct{})
	for _, key := range writeKeys {
		index := locks.spread(key)
		writeIndexSet[index] = struct{}{}
	}

	for i := len(indices) - 1; i >= 0; i-- {
		index := indices[i]
		if _, ok := writeIndexSet[index]; ok {
			locks.table[index].Unlock()
		} else {
			locks.table[index].RUnlock()
		}
	}
}
