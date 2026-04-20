package set

import "go-Redis/datastruct/dict"

type Set struct {
	dict dict.Dict
}

func Make(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimple(),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}
func MakeConcurrentSafe(members ...string) *Set {
	set := &Set{
		dict: dict.MakeConcurrent(1),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}
func (s *Set) Add(member string) int {
	return s.dict.Put(member, nil)
}
func (s *Set) Remove(member string) int {
	_, result := s.dict.Remove(member)
	return result
}
func (s *Set) Has(member string) bool {
	_, exists := s.dict.Get(member)
	return exists
}

func (s *Set) Len() int {
	return s.dict.Len()
}
func (s *Set) Members() []string {
	res := make([]string, 0, s.Len())
	s.dict.ForEach(func(key string, val any) bool {
		res = append(res, key)
		return true
	})
	return res
}
