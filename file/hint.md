# Pager

pager 负责进行文件的页管理

包括：读写， cache， lru

cache 作为缓存

lru 永远把最新的放在最前面

之后就可以知道谁在最后面，然后在 cache 里面把他干掉

## dirty page

dirty page 和 cache 可能会使用同样的存储结构进行存储，他们也都在内存里面

但这并不意味着他们的可以混同，他们的目的是不一样的
    
    cache 的目的是被查询（或者说作为磁盘中内容的高速备份）

    dirtypage 的目的是批量写入，减少 io 次数

## lsn (logSequenceNumber)
