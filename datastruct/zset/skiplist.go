package zset

import (
	"math/bits"
	"math/rand"
)

const maxLevel = 16

type Level struct {
	forward *node
	span    int64
}
type node struct {
	Element  *Element
	backward *node
	level    []*Level
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: &Element{
			Member: member,
			Score:  score,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = &Level{}
	}
	return n
}

type SkipList struct {
	header *node
	tail   *node
	length int64
	level  int16
}

func MakeSkipList() *SkipList {
	header := makeNode(maxLevel, 0, "")
	return &SkipList{
		header: header,
		level:  1,
		length: 0,
	}
}
func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}
func (s *SkipList) insert(member string, score float64) *node {
	//记录每一层里“插入位置前一个节点是谁”
	update := make([]*node, maxLevel)
	//记录走到这个位置时，已经跨过多少个底层元素
	rank := make([]int64, maxLevel)
	n := s.header
	for i := s.level - 1; i >= 0; i-- {
		if i == s.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for n.level[i].forward != nil && (n.level[i].forward.Element.Score < score ||
			(n.level[i].forward.Element.Score == score &&
				n.level[i].forward.Element.Member < member)) {
			rank[i] += n.level[i].span
			n = n.level[i].forward
		}
		update[i] = n
	}
	level := randomLevel()
	if level > s.level {
		for i := s.level; i < level; i++ {
			rank[i] = 0
			update[i] = s.header
			update[i].level[i].span = s.length
		}
		s.level = level
	}
	n = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		n.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = n
		n.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	for i := level; i < s.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == s.header {
		n.backward = nil
	} else {
		n.backward = update[0]
	}

	if n.level[0].forward != nil {
		n.level[0].forward.backward = n
	} else {
		s.tail = n
	}

	s.length++
	return n
}
func (s *SkipList) removeNode(n *node, update []*node) {
	for i := int16(0); i < s.level; i++ {
		if update[i].level[i].forward == n {
			update[i].level[i].span += n.level[i].span - 1
			update[i].level[i].forward = n.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if n.level[0].forward != nil {
		n.level[0].forward.backward = n.backward
	} else {
		s.tail = n.backward
	}
	for s.level > 1 && s.header.level[s.level-1].forward == nil {
		s.level--
	}
	s.length--
}
func (s *SkipList) remove(member string, score float64) bool {
	update := make([]*node, maxLevel)
	n := s.header
	for i := s.level - 1; i >= 0; i-- {
		for n.level[i].forward != nil &&
			(n.level[i].forward.Element.Score < score ||
				(n.level[i].forward.Element.Score == score &&
					n.level[i].forward.Element.Member < member)) {
			n = n.level[i].forward
		}
		update[i] = n
	}
	n = n.level[0].forward
	if n != nil && n.Element.Score == score && n.Element.Member == member {
		s.removeNode(n, update)
		return true
	}
	return false
}
func (s *SkipList) getByRank(rank int64) *node {
	if rank <= 0 || rank > s.length {
		return nil
	}
	var i int64 = 0
	n := s.header
	for level := s.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span <= rank) {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}
