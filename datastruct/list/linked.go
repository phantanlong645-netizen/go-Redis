package list

type node struct {
	val  any
	prev *node
	next *node
}
type LinkedList struct {
	first *node
	last  *node
	size  int
}

func Make(vals ...any) *LinkedList {
	list := &LinkedList{}
	for _, v := range vals {
		list.Add(v)
	}
	return list
}

// 尾插法
func (list *LinkedList) Add(val any) {
	n := &node{val: val}
	if list.last == nil {
		list.first = n
		list.last = n
		list.size++
		return
	}
	n.prev = list.last
	list.last.next = n
	list.last = n
	list.size++
}
func (list *LinkedList) find(index int) *node {
	if index < list.size/2 {
		n := list.first
		for i := 0; i < index; i++ {
			n = n.next
		}
		return n
	}
	n := list.last
	for i := list.size - 1; i > index; i-- {
		n = n.prev
	}
	return n
}
func (list *LinkedList) Get(index int) any {
	if index < 0 || index >= list.size {
		panic("index out of bound")
	}
	return list.find(index).val
}
func (list *LinkedList) Set(index int, val any) {
	if index < 0 || index >= list.size {
		panic("index out of bound")
	}
	n := list.find(index)
	n.val = val
}
func (list *LinkedList) Len() int {
	return list.size
}

func (list *LinkedList) ForEach(consumer Consumer) {
	i := 0
	for cur := list.first; cur != nil; cur = cur.next {
		if !consumer(i, cur.val) {
			return
		}
		i++
	}
}
func (list *LinkedList) Contains(expected Expected) bool {
	for cur := list.first; cur != nil; cur = cur.next {
		if expected(cur.val) {
			return true
		}
	}
	return false
}
func (list *LinkedList) RemoveAllByVal(expected Expected) int {
	removed := 0
	for cur := list.first; cur != nil; {
		next := cur.next
		if expected(cur.val) {
			if cur.prev != nil {
				cur.prev.next = next
			} else {
				list.first = next
			}
			if next != nil {
				next.prev = cur.prev
			} else {
				list.last = cur.prev
			}
			removed++
			list.size--
		}
		cur = next
	}
	return removed
}
func (list *LinkedList) Insert(index int, val any) {
	if index < 0 || index > list.size {
		panic("index out of bound")
	}
	if index == list.size {
		list.Add(val)
		return
	}
	cur := list.find(index)
	node := &node{
		val:  val,
		prev: cur.prev,
		next: cur,
	}
	if cur.prev == nil {
		list.first = node
	} else {
		cur.prev.next = node
	}
	cur.prev = node
	list.size++
}
func (list *LinkedList) removeNode(n *node) {
	if n.prev == nil {
		list.first = n.next
	} else {
		n.prev.next = n.next
	}
	if n.next == nil {
		list.last = n.prev
	} else {
		n.next.prev = n.prev
	}
	list.size--
	n.prev = nil
	n.next = nil
}
func (list *LinkedList) Remove(index int) any {
	if index < 0 || index >= list.size {
		panic("index out of bound")
	}
	n := list.find(index)
	list.removeNode(n)
	return n.val
}
func (list *LinkedList) RemoveLast() any {
	if list.last == nil {
		return nil
	}
	n := list.last
	list.removeNode(n)
	return n.val
}
func (list *LinkedList) Range(start int, stop int) []any {
	if start < 0 || start >= list.size {
		panic("start out of range")
	}
	if stop < start || stop > list.size {
		panic("stop out of range")
	}

	res := make([]any, 0, stop-start)
	cur := list.find(start)
	for i := start; i < stop; i++ {
		res = append(res, cur.val)
		cur = cur.next
	}
	return res
}
func (list *LinkedList) ReverseRemoveByVal(expected Expected, count int) int {
	removed := 0
	for cur := list.last; cur != nil; {
		prev := cur.prev
		if expected(cur.val) {
			list.removeNode(cur)
			removed++
			if removed == count {
				break
			}
		}
		cur = prev
	}
	return removed
}
func (list *LinkedList) RemoveByVal(expected Expected, count int) int {
	removed := 0
	for cur := list.first; cur != nil; {
		next := cur.next
		if expected(cur.val) {
			list.removeNode(cur)
			removed++
			if removed == count {
				break
			}
		}
		cur = next
	}
	return removed
}
