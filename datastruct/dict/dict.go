package dict

import (
	"hash/fnv"
	"sync"
)

type Consumer func(key string, val any) bool

type Dict interface {
	Get(key string) (val any, exist bool)
	Put(key string, val any) int
	Remove(key string) (val any, result int)
	PutIfAbsent(key string, val any) int
	PutIfExists(key string, val any) int
	Len() int
	ForEach(consumer Consumer)
}
type shard struct {
	m     map[string]any
	mutex sync.RWMutex
}
type ConcurrentDict struct {
	table      []*shard
	count      int
	shardCount int
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	if shardCount <= 0 {
		shardCount = 1
	}
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]any),
		}
	}
	return &ConcurrentDict{
		table:      table,
		shardCount: shardCount,
		count:      0,
	}

}
func (d *ConcurrentDict) spread(key string) uint32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return hash.Sum32() % uint32(len(d.table))
}
func (d *ConcurrentDict) getShard(index uint32) *shard {

	return d.table[index]
}
func (d *ConcurrentDict) Get(key string) (val any, exists bool) {
	index := d.spread(key)
	shard := d.getShard(index)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	val, ok := shard.m[key]
	return val, ok

}
func (d *ConcurrentDict) Put(key string, val any) int {
	index := d.spread(key)
	shard := d.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	_, ok := shard.m[key]
	shard.m[key] = val
	if !ok {
		d.count++
		return 1
	} else {
		return 0
	}
}
func (d *ConcurrentDict) Remove(key string) (val any, result int) {
	index := d.spread(key)
	shard := d.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	val, ok := shard.m[key]
	if ok {
		delete(shard.m, key)
		d.count--
		return val, 1
	}
	return nil, 0
}
func (d *ConcurrentDict) Len() int {
	return d.count
}
func (d *ConcurrentDict) PutIfAbsent(key string, val any) int {
	index := d.spread(key)
	s := d.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, existed := s.m[key]; existed {
		return 0
	}
	s.m[key] = val
	d.count++
	return 1
}
func (d *ConcurrentDict) PutIfExists(key string, val any) int {
	index := d.spread(key)
	s := d.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, existed := s.m[key]; !existed {
		return 0
	}
	s.m[key] = val
	return 1
}
func (d *ConcurrentDict) ForEach(consumer Consumer) {
	if consumer == nil {
		return
	}
	for _, shard := range d.table {
		shard.mutex.RLock()
		stop := false
		for key, v := range shard.m {
			if !consumer(key, v) {
				stop = true
				break
			}
		}
		shard.mutex.RUnlock()
		if stop {
			break
		}
	}

}
