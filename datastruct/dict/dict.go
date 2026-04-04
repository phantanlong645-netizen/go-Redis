package dict

import "sync"

type Dict interface {
	Get(key string) (val any, exist bool)
	Put(key string, val any) int
	Remove(key string) int
}
type ConcurrentDict struct {
	mu sync.RWMutex
	m  map[string]any
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	return &ConcurrentDict{
		m: make(map[string]any),
	}
}
func (d *ConcurrentDict) Get(key string) (val any, exists bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, exists = d.m[key]
	return
}
func (d *ConcurrentDict) Put(key string, val any) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, existed := d.m[key]
	d.m[key] = val
	if existed {
		return 0
	}
	return 1
}
func (d *ConcurrentDict) Remove(key string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, existed := d.m[key]
	if existed {
		delete(d.m, key)
		return 1
	}
	return 0
}
