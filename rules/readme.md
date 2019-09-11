### 规则模块
    在主函数main中启动了rulemanage，当运行Run()时，解除block()阻塞，开始运行告警模块。在设置启动时间之后，
    启动断续器，循环执行Eval()。Eval每次执行一次所有规则。rule的Eval()是一个interface，
    alert rule的eval在alerting.go中实现。由rule的query result和rule expression进行判断,是新建/更新告警。
    
    
### Keys

1. Run()和Update()
 
    Update()启动/启动告警管理器,从配置文件中加载规则组，但会block执行方法(不执行eval函数)，
    main函数调用run()时才解除block
    
2. Group    
    group = rule group
    
3. Rule
    rule分为recording rule and alert rule。每个类型都有对应的执行函数eval()
    
4. manager.Eval()
    执行eval函数，写入appender。如果是告警规则，则发送告警。
    
5. staleSeries
    没理解，下次补充
    
    
    


