package zset

type Element struct {
	Member string
	Score  float64
}
type SortedSet struct {
	dict     map[string]*Element
	skiplist *SkipList
}

func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: MakeSkipList(),
	}
}
func (s *SortedSet) Add(member string, score float64) bool {
	element, ok := s.dict[member]
	if ok {
		if element.Score == score {
			return false
		}
		s.skiplist.remove(member, element.Score)
	}
	s.skiplist.insert(member, score)
	s.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	return !ok
}
func (s *SortedSet) Len() int64 {
	return int64(len(s.dict))
}
func (s *SortedSet) Get(member string) (*Element, bool) {
	element, ok := s.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

func (s *SortedSet) Remove(member string) bool {
	element, ok := s.dict[member]
	if !ok {
		return false
	}
	ok = s.skiplist.remove(member, element.Score)
	if !ok {
		return false
	}
	delete(s.dict, member)
	return true
}
func (s *SortedSet) GetByRank(rank int64) (*Element, bool) {
	n := s.skiplist.getByRank(rank)
	if n == nil {
		return nil, false
	}
	return n.Element, true
}
