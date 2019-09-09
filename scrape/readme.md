### 数据采集

#### Target

#### Description
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
    

### 