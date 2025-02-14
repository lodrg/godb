package file

import "container/list"

// LRU 管理最近最少使用的页面
type lru struct {
	capacity int
	list     *list.List            // 双向链表用于维护顺序
	items    map[int]*list.Element // 用于快速查找
}

// 创建新的 LRU
func newLRU(capacity int) *lru {
	return &lru{
		capacity: capacity,
		list:     list.New(),
		items:    make(map[int]*list.Element),
	}
}

// 添加或更新页面号
func (l *lru) add(pageNum int) {
	if elem, exists := l.items[pageNum]; exists {
		l.list.MoveToFront(elem)
		return
	}

	//// 如果达到容量限制，删除最久未使用的页面
	//if len(l.items) >= l.capacity {
	//	oldest := l.list.Back()
	//	if oldest != nil {
	//		delete(l.items, oldest.Value.(int))
	//		l.list.Remove(oldest)
	//	}
	//}

	// 添加新页面到前端
	elem := l.list.PushFront(pageNum)
	l.items[pageNum] = elem
}

// 检查页面是否在 LRU 中
func (l *lru) contains(pageNum int) bool {
	_, exists := l.items[pageNum]
	return exists
}

// 获取要被移除的页面号
func (l *lru) getEvict() (int, bool) {
	if l.list.Len() == 0 {
		return 0, false
	}
	oldest := l.list.Back()
	return oldest.Value.(int), true
}

// 删除指定页面
func (l *lru) remove(pageNum int) {
	if elem, exists := l.items[pageNum]; exists {
		delete(l.items, pageNum)
		l.list.Remove(elem)
	}
}

// removeLast 移除并返回最后一个元素
func (l *lru) removeLast() (int, bool) {
	if l.list.Len() == 0 {
		return 0, false
	}

	lastElement := l.list.Back()
	pageNum := lastElement.Value.(int)

	// 从 map 和链表中删除
	delete(l.items, pageNum)
	l.list.Remove(lastElement)

	return pageNum, true
}

func (l *lru) Close() error {
	// 清空链表
	l.list.Init()
	// 清空映射
	l.items = make(map[int]*list.Element)
	return nil
}
