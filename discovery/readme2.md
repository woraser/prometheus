### 服务发现组件 Service Discovery(SD)
    动态发现采集目标，进行数据更新，替换。
    根据README文件，现有的sd组件应该不再更新，未来会有新的sd组件来代替它
    
[流程图](https://www.processon.com/view/link/5d80440de4b0b018f3f2b303)
1. 在main函数中创建discoveryManager，应用配置文件
2. 注册服务发现的监控项（总数，失败数量，更新数量等）
3. 支持不同的服务发现组件 etcd，consule等  registerProviders()
4. 根据服务发现组件发来的数据，更新本地数据

### Keys
1. provider

    提供服务发现的组件，例如consule，file...。实现discovery接口
    
2. targetgroup
    
    目标组
    
3. triggerSend
    触发器，用于告知manager,从provider收到新的更新内容
    
4. syncCh
    targetgroup同步通道，通知scrapeManager目标组发生了变化
