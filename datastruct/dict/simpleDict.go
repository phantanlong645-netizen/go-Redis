package dict

type SimpleDict struct {
	m map[string]any
}

func MakeSimple() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]any),
	}
}

func (d *SimpleDict) Get(key string) (val any, exists bool) {
	val, exists = d.m[key]
	return
}

func (d *SimpleDict) Put(key string, val any) int {
	_, exists := d.m[key]
	d.m[key] = val
	if exists {
		return 0
	}
	return 1
}

func (d *SimpleDict) PutIfAbsent(key string, val any) int {
	if _, exists := d.m[key]; exists {
		return 0
	}
	d.m[key] = val
	return 1
}

func (d *SimpleDict) PutIfExists(key string, val any) int {
	if _, exists := d.m[key]; !exists {
		return 0
	}
	d.m[key] = val
	return 1
}

func (d *SimpleDict) Remove(key string) (val any, result int) {
	val, exists := d.m[key]
	if !exists {
		return nil, 0
	}
	delete(d.m, key)
	return val, 1
}

func (d *SimpleDict) Len() int {
	return len(d.m)
}
func (d *SimpleDict) ForEach(consumer Consumer) {
	if consumer == nil {
		return
	}
	for key, val := range d.m {
		if !consumer(key, val) {
			return
		}
	}
}
