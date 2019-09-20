### 存储支持模块

    定义存储接口的方法和结构体,tsdb是一个具体的实现。

1. MinBlockDuration/maxBlockDuration

    数据在内存中保留时间，最大和最小值默认是2h/36h，推荐26h。
    
    
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

    inerface.go中定义的存储接口的tsdb实现。启动tsdb之后，根据配置项打开存储引擎，
    并实现接口方法。通过适配器(adapter)的方式来实现接口。
    
    
9. buffer.go

    数据缓存,提供一些相关的方法。