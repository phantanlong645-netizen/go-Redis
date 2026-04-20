package list

import "container/list"

const pageSize = 1024

type QuickList struct {
	data *list.List
	size int
}

type iterator struct {
	node   *list.Element
	offset int
	ql     *QuickList
}

func NewQuickList() *QuickList {
	return &QuickList{
		data: list.New(),
	}
}

func (iter *iterator) page() []any {
	return iter.node.Value.([]any)
}

func (iter *iterator) get() any {
	return iter.page()[iter.offset]
}

func (iter *iterator) set(val any) {
	page := iter.page()
	page[iter.offset] = val
}

func (iter *iterator) next() bool {
	page := iter.page()
	if iter.offset < len(page)-1 {
		iter.offset++
		return true
	}

	if iter.node == iter.ql.data.Back() {
		iter.offset = len(page)
		return false
	}

	iter.node = iter.node.Next()
	iter.offset = 0
	return true
}

func (iter *iterator) prev() bool {
	if iter.offset > 0 {
		iter.offset--
		return true
	}

	if iter.node == iter.ql.data.Front() {
		iter.offset = -1
		return false
	}

	iter.node = iter.node.Prev()
	prevPage := iter.node.Value.([]any)
	iter.offset = len(prevPage) - 1
	return true
}

func (iter *iterator) atEnd() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Back() {
		return false
	}
	page := iter.page()
	return iter.offset == len(page)
}

func (iter *iterator) atBegin() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Front() {
		return false
	}
	return iter.offset == -1
}

func (iter *iterator) remove() any {
	page := iter.page()
	val := page[iter.offset]
	page = append(page[:iter.offset], page[iter.offset+1:]...)

	if len(page) > 0 {
		iter.node.Value = page
		if iter.offset == len(page) {
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
		}
	} else {
		if iter.node == iter.ql.data.Back() {
			if prevNode := iter.node.Prev(); prevNode != nil {
				iter.ql.data.Remove(iter.node)
				iter.node = prevNode
				iter.offset = len(prevNode.Value.([]any))
			} else {
				iter.ql.data.Remove(iter.node)
				iter.node = nil
				iter.offset = 0
			}
		} else {
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}

	iter.ql.size--
	return val
}

func (ql *QuickList) Add(val any) {
	ql.size++
	if ql.data.Len() == 0 {
		page := make([]any, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}

	backNode := ql.data.Back()
	backPage := backNode.Value.([]any)
	if len(backPage) == cap(backPage) {
		page := make([]any, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}

	backPage = append(backPage, val)
	backNode.Value = backPage
}

func (ql *QuickList) find(index int) *iterator {
	if ql == nil {
		panic("list is nil")
	}
	if index < 0 || index >= ql.size {
		panic("index out of bound")
	}

	var n *list.Element
	var page []any
	var pageBeg int

	if index < ql.size/2 {
		n = ql.data.Front()
		pageBeg = 0
		for {
			page = n.Value.([]any)
			if pageBeg+len(page) > index {
				break
			}
			pageBeg += len(page)
			n = n.Next()
		}
	} else {
		n = ql.data.Back()
		pageBeg = ql.size
		for {
			page = n.Value.([]any)
			pageBeg -= len(page)
			if pageBeg <= index {
				break
			}
			n = n.Prev()
		}
	}

	pageOffset := index - pageBeg
	return &iterator{
		node:   n,
		offset: pageOffset,
		ql:     ql,
	}
}

func (ql *QuickList) Get(index int) any {
	iter := ql.find(index)
	return iter.get()
}

func (ql *QuickList) Set(index int, val any) {
	iter := ql.find(index)
	iter.set(val)
}

func (ql *QuickList) Insert(index int, val any) {
	if index == ql.size {
		ql.Add(val)
		return
	}

	iter := ql.find(index)
	page := iter.node.Value.([]any)
	if len(page) < pageSize {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
		iter.node.Value = page
		ql.size++
		return
	}

	var nextPage []any
	nextPage = append(nextPage, page[pageSize/2:]...)
	page = page[:pageSize/2]

	if iter.offset < len(page) {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
	} else {
		i := iter.offset - pageSize/2
		nextPage = append(nextPage[:i+1], nextPage[i:]...)
		nextPage[i] = val
	}

	iter.node.Value = page
	ql.data.InsertAfter(nextPage, iter.node)
	ql.size++
}

func (ql *QuickList) Remove(index int) any {
	iter := ql.find(index)
	return iter.remove()
}

func (ql *QuickList) Len() int {
	return ql.size
}

func (ql *QuickList) RemoveLast() any {
	if ql.Len() == 0 {
		return nil
	}

	ql.size--
	lastNode := ql.data.Back()
	lastPage := lastNode.Value.([]any)

	if len(lastPage) == 1 {
		ql.data.Remove(lastNode)
		return lastPage[0]
	}

	val := lastPage[len(lastPage)-1]
	lastPage = lastPage[:len(lastPage)-1]
	lastNode.Value = lastPage
	return val
}

func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	if ql.size == 0 {
		return 0
	}

	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}

func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}

	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		} else {
			iter.next()
		}
	}
	return removed
}

func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}

	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		iter.prev()
	}
	return removed
}

func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		panic("list is nil")
	}
	if ql.Len() == 0 {
		return
	}

	iter := ql.find(0)
	i := 0
	for {
		goNext := consumer(i, iter.get())
		if !goNext {
			break
		}
		i++
		if !iter.next() {
			break
		}
	}
}

func (ql *QuickList) Contains(expected Expected) bool {
	contains := false
	ql.ForEach(func(i int, actual any) bool {
		if expected(actual) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

func (ql *QuickList) Range(start int, stop int) []any {
	if start < 0 || start >= ql.Len() {
		panic("start out of range")
	}
	if stop < start || stop > ql.Len() {
		panic("stop out of range")
	}

	sliceSize := stop - start
	res := make([]any, 0, sliceSize)
	iter := ql.find(start)
	for i := 0; i < sliceSize; i++ {
		res = append(res, iter.get())
		iter.next()
	}
	return res
}
