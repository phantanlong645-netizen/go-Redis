package list

type node struct {
	val  any
	prev *node
	next *node
}
type LinkedList struct {
	head *node
	tail *node
	size int
}
type Consumer func(i int, v any) bool

func Make() *LinkedList {
	return &LinkedList{}
}
func (l *LinkedList) Add(val any) {
	n := &node{val: val}
	if l.tail == nil {
		l.head = n
		l.tail = n
		l.size++
		return
	}
	n.prev = l.tail
	l.tail.next = n
	l.tail = n
	l.size++
}
func (l *LinkedList) Len() int {
	return l.size
}
func (l *LinkedList) Contains(expected func(a any) bool) bool {
	for cur := l.head; cur != nil; cur = cur.next {
		if expected(cur.val) {
			return true
		}
	}
	return false
}
func (l *LinkedList) ForEach(consumer Consumer) {
	i := 0
	for cur := l.head; cur != nil; cur = cur.next {
		if !consumer(i, cur.val) {
			return
		}
		i++
	}
}
func (l *LinkedList) RemoveAllByVal(expected func(a any) bool) int {
	removed := 0
	for cur := l.head; cur != nil; {
		next := cur.next
		if expected(cur.val) {
			if cur.prev != nil {
				cur.prev.next = next
			} else {
				l.head = cur.next
			}
			if cur.next != nil {
				cur.next.prev = cur.prev
			} else {
				l.tail = cur.prev
			}
			l.size--
			removed++
		}
		cur = next
	}
	return removed
}
