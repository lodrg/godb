# godb
go version database frome scratch

pseudo 01

baseNode
    keys keycount 
leafNode
    nextleaf

pseudo 02


pseudo 03

一个 bpt 类，属性是 root order
一个 BaseNode 类，属性是 keys keycount parent 
（逻辑上来说只要有 key 就可以了，keycount 可以被算出来
如何要存的是键值对的话还需要对这个结构的定义）
需要给 node 实现的方式是 insert 和 search

然后是 internalnode 和 leafnode
internalnode 里面多了一个 children 属性，存的是它的子节点
leafnode 里面多了一个 nextleaf 属性，存的是他的下一个叶子节点

这几个类都需要构造方法
bpt internalnode 和 leafnode 需要插入和搜索方法

bpt 的插入方法
    如果没有root 就新建一个叶子节点
    如果有了已经， 就直接调用 bn 接口的 insert
    每次调用 insert 都需要重新看一下 root 指向的位置，这里用 insert 的返回值返回上来

internalnode 的 insert
    参数：key vlaue
    返回：Node

    首先要找到要插入的子节点
    先找到比当前节点比他大的节点，如果不是大于 0 的话就往回走一个

    然后递归插入子节点（这里子节点可能是中间节点也可能是叶子节点）

    如果子节点没有分裂（通过返回的节点还是不是子节点来判断）就返回本节点

    如果子节点分裂了就要处理分裂状况

insertChild 处理分裂情况
    参数：newChild 新节点 insertindex 插入的位置
    返回：Node

    判断返回的子节点的type
    如果是中间节点的话:
        把返回的第一个 key，插入中间节点的 entry
        然后维护一下中间节点 children 情况,这里要更新分裂的子节点的两个children，因为分裂后的领头节点可能变了

    如果是叶子节点的话：
        直接把 entry 和 children 加进去

    然后检查现在的节点是否要分裂
        如果要分裂直接 split
        分裂的话返回父节点

    不分裂正常返回本节点

中间节点分裂：
    参数：没
    返回：Node

    和叶子节点的分裂相似
    先创建新节点，然后按 order 的一般把数据分开放

    然后更新的是当前节点的状态（之前要维护叶子节点的链表）

    最后新建父节点，把父节点的属性安排好

    最后返回

internal 的 search:
    参数：key
    返回：值，bool

    主要是找到适合的子节点然后递归查询


leafnode 的 insert
    参数：key value
    返回：Node

    首先找到 insert 的位置
    下标小于元素容量，并且key大于已经有的 key 的时候就继续增加 index
    直到下标超过元素容量，或者key 不大于已经有的可以了

    这时候如果key 相等，也就是 key 已经存在，那么直接更新值(这其实是 update)
    这时候直接返回当前节点（有可能作为 root）

    如果不是已经存在的话进行插入逻辑三段，先加空结构，然后 copy，最后在目的位置加入

    最后如果元素数量超过 order，进行分裂
    分裂的话就返回分裂后的父节点

    不然直接返回本节点

分裂函数
    参数：没有
    返回：node（基本是分裂后父节点）

    首先要直到怎么分 中间index 就是 (order-1)/2

    然后创建右侧节点（往右裂）
    创建右侧节点后，把数据分好，原节点的放原节点，新节点的放新节点

    维护一下两个叶子节点的 next 属性

    创建父节点，并维护父节点的entry 和 children

    最后返回父节点


leafnode 的 search
    参数：key
    返回：value ，bool

    直接遍历 entries 如果有 key 相同的就是了
    （可以进行二分查找优化？）

