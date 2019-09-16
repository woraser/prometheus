### 可执行文件的CLI 逻辑

#### prometheus 主程序
1. 可执行文件prometheus的cli 逻辑
2. 各组件的初始化设置，设置默认值
3. 创建各组件的管理器，比如notifyManager,discoveryManager,scrapeManager等。
4. 通过oklog的group来同时启动所有组件(并发执行)

#### promtool 监控系统工具

1. debug
2. 校验配置文件，路径等
3. 压缩工具
4. 单元测试
