```
internalNode
    order: int
    pageNumber: int             (*)
    keys: []int
    values: [][]byte
    childrenPageNumbers []int   (*)
    DiskPager
leafNode
    order:              int
    pageNumber:         int     (*)
    keys:               []int
    values:                [][]byte
    NextPageNumber        int   (*)
    DiskPager
bptree
    order:                 int
    DiskPager:
    rootPageNumber:        int
    valueLength:           int
    redoLog:
```


format for leafNode
```
|isLeaf (1 byte) | keyCount (4 bytes) | [key (4 bytes) | valueLength (4 bytes) | valueData (valueLength bytes)] * keyCount | nextPageNumber (4 bytes)
```
```
000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000
```


## 从内存到磁盘的转换过程遇到了很大问题，递归情况的代码定位问题非常困难
1. 需要规划和写单测
2. 对于数据产生的情况要有清楚的认识（这也是一种接口定义）
3. 对于数据类型的选择要谨慎（基于 2）

在当下的情况下，对于落盘情况的把握是比较关键的
metadata 的情况

## 如何理解 node 和 pager 的关系？
首先一个 tree 有一个 pager，这点在 tree 的定义中可以明确，leafNode 和 interanlNode 的 pager 都引用的 tree 的 pager

一个 pager 里面有多个 page

一个 tree 里面有多个 node

所以在 node 里面我们会以 pageNumber 来标记当前 node 所在的 page

同时在 root 里面使用 rooPageNumber 来标记根节点所在的 page

新建 tree 的时候

    会把 page 安排好, 新建 pager，分配一个新 page，并在这个新 page 上新建一个 leaf 作为 root
    如果是 tree 里面已经有内容了，就直接把属性归位就好

插入 tree 的时候

    需要先根据 root 所在的 page 获取 root, 然后向 root 里面 insert，这时候会根据 root 的 node 类型进行插入
        如果是 internalNode 需要根据 key 寻址 key 的子节点所在的位置，然后根据子节点位置获得子节点，向字节点 insert （可能会递归）
        如果是 leafNode 这个时候直接插入就可以，然后就是分裂的判断
            如果需要分裂(key 数量超过 order），就需要把子节点本身分裂，返回右边节点的第一个 key
            父节点获得这个 key 之后，再判断自己是否需要分裂，如果需要分裂重复这个过程
            指导最后 root 节点分裂会产出新的 root 节点，这个时候就需要从新写元数据
            
所以 node 和 page 的关系是一一对应的，每次都需要 pageNumber 来获取实际的 page，从而构建 node
            
## redolog 的问题
当前的 redolog 是在 tree 层面实现的，每次在 tree 层面进行操作的时候会在操作前写入 redolog，操作后把 exec 位置 1
这样的问题是没办法和脏页或者 checkpoint 的逻辑配合，脏页的逻辑需要 redolog 在page 的层面上记录
