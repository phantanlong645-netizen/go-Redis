package set

type Set struct {
	dict map[string]struct{}
}

func Make() *Set {
	return &Set{
		dict: make(map[string]struct{}),
	}
}
func (s *Set) Add(member string) int {
	if _, exist := s.dict[member]; exist {
		return 0
	}
	s.dict[member] = struct{}{}
	return 1
}
func (s *Set) Remove(member string) int {
	if _, exist := s.dict[member]; !exist {
		return 0
	}
	delete(s.dict, member)
	return 1
}
func (s *Set) Has(member string) bool {
	_, exists := s.dict[member]
	return exists
}

func (s *Set) Len() int {
	return len(s.dict)
}
