### 存储支持模块

    定义存储接口的方法和结构体，所有实现接口的方法都能接入存储。

1. MinBlockDuration

    block的最小生存时间
    
2. interface.go 

    storage接口定义，所有的存储实现都需要实现这个接口类。实现写入，查询等方法。
    
3. appender

    写入器，用于批量写入数据。支持常规写入和缓存写入二种模式。支持回滚。
    
4. fanout.go

    存储的抽象层。包括一个主存储和多个二级存储器。它实现了interface.go的方法，外部可以直接使用。
    同时也实现了高级api接口。
    
5. SeriesIterator

    时序数据迭代，interface.go中定义了接口，fanout.go中的mergeIterator实现该接口。
    
6. Select()

    核心方法，根据给定查询条件中存储中查询返回结果。
    mergeQuerier中的每一个querier执行查询，合并返回结果。
    
7. NoopQuerier
    
    类似占位符，代表一个空的查询器，不做实事
    
8. tsdb.go

    tsdb对interface.go中定义接口的实现