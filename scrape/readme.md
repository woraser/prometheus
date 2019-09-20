### 数据采集
    https://www.processon.com/view/link/5d8330a1e4b0ca6e051e9bb5

#### Manage
    
    Scrape管理器，主函数run()，监听外部传入的taregts channel；发生变化则更新scrapePool set, 触发reload()事件
    ScrapePool是针对单个target的scrape管理器，target中discovery模块但targetGroup中获取。
    断续运行，通过http GET请求获取metric的原始数据，将数据写入数据缓冲区，执行持久化处理。    
    
##### Keys

1. run()
    
    入口方法，在main.go中调用，启动manager。
    
2. reloader()
    
    核心方法，启动后台服务，根据传入的targetgroup变化及时跟新scrapePool。


#### Target

    从discovery的target group中获取数据采集的target集合。
    合并元数据label和自定义label，relabel之后删除元数据label。
    对最终结果进行utf8校验。

##### Keys

1. timeLimitAppender
    
    timeLimitAppender是用来限制data的时效性的。
    如果某一条数据在提交给storage进行存储的时候，生成这条数据已经超过10分钟，
    那么prometheus就会抛错。目前10分钟是写死到代码里面的， 
    无法通过配置文件配置。
    
2. limit Appender

    limitAppender是用来限制存储的数据label个数，
    如果超过限制，该数据将被忽略，不入存储；默认值为0，
    表示没有限制,可以通过配置文件中的sample_limit来进行配置。
    
3. lastScrape

    最后一次采集时间。

4. lastScrapeDuration

    最后一次采集的持续时间。
    
5. lastError

    最后一次采集是抛出的错误，没有则为nil
    

### Scrape

#### Description
    为每一个target创建scrapeloop，通过get请求采集数据，延迟写入appender中。
    reload方法会更新target。当targetgtoup发生变化时，manager.go中触发sync()方法。
    写入过程中用到了cache。

#### Keys

1. scrapePool

    数据采集管理器

2. maxAheadTime

    集合的的数据管理器appender的最大生存时间，默认是10分钟

3. loop

    单向启动器
    
4. scrapeCache

    scrape缓存，具体逻辑在storage模块中
    
5. scrapeLoop
    每一个target的scrape管理器，实现了loop接口   

    



