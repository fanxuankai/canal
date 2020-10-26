## 简介
一个基于 [alibaba/canal](https://github.com/alibaba/canal) 的数据消费组件, 只需简单配置即可自动消费 MySQL 数据库的数据。

![](http://processon.com/chart_image/5eda399f0791297145d394b3.png)

## 功能
- **Redis**：实时刷新缓存 [canal-redis-boot-starter](https://github.com/fanxuankai/canal/tree/main/canal-boot-starter/canal-redis-boot-starter)
- **Elasticsearch**：自动构建全文搜索 [canal-es-boot-starter](https://github.com/fanxuankai/canal/tree/main/canal-boot-starter/canal-elasticsearch-boot-starter)
- **DB**：数据库同步 [canal-mysql-boot-starter](https://github.com/fanxuankai/canal/tree/main/canal-boot-starter/canal-mysql-boot-starter)
- **MQ**：数据监听, 自动下发任务, 目前支持 RabbitMQ、XXL-MQ、RocketMQ、Kafka [canal-rabbitmq-boot-starter](https://github.com/fanxuankai/canal/tree/main/canal-boot-starter/canal-rabbitmq-boot-starter)

## 运行环境
- Canal 服务
- Java 8
- Spring Boot
- Redis

## 基础配置
```yml
## application.yml
spring:
  redis:
    host: localhost
    port: 6379
canal:
  # redis、elasticsearch、mysql、mq
  redis:
    # 是否开启 canal 服务
    enabled: true
    configuration:
      # 应用 id, 用于 canal client 抢占式运行、binlog offset 防重, 默认取 instance
      id: example-service
      # 集群配置
      #cluster:
        # zookeeper的ip+端口, 以逗号隔开
        #nodes: localhost:2181,localhost:2182,localhost:2183
      # 单节点配置
      #single-node:
        # ip
        #hostname: localhost
        # 端口
        #port: 11111
      # 实例
      #instance: example
      # 过滤
      #filter: .*\\..*
      # 账号
      #username: canal
      # 密码
      #password: canal
      # 拉取数据的间隔 ms
      #interval-millis: 1000
      # 拉取数据的数量
      #batch-size: 100
      # 打印事件日志
      #show-event-log: false
      # 打印 Entry 日志
      #show-entry-log: false
      # 打印数据明细日志
      #show-row-change: false
      # 格式化数据明细日志
      #format-row-change-log: false
      # 批次达到一定数量进行并行处理, 且确保顺序消费
      #performance-threshold: 10000
      # 跳过处理
      #skip: false
      # 全局逻辑删除字段
      #logic-delete-field: deleted
      # 激活逻辑删除
      #enable-logic-delete: false
```

## 常见问题
- 删表时, Redis 暂支持单表删除, 一次删除一张表 Redis 可以正常同步, 一次删除多张表 Redis 只同步第一张表
- canal 不支持数据库 bit 格式
- 同步到 Redis 时用组合键, 发现 MySQL 与 Redis 数量不对
    - 原因是数据库排序规则不区分大小写, 导致 MySQL 的数量比 Redis 少
    - 如果需要区分大小写建议使用 utf8mb4_bin
    - 修改为 CHARSET=utf8mb4 COLLATE=utf8mb4_bin, 验证结果完全一致
- 引入 spring-boot-devtools 导致无法注入
    - 需要去掉此依赖, 否则会导致部分 bean 无法注入
- 创建 canal 实例时, 建议指定 filter 参数
- Channel shutdown: clean channel shutdown; protocol method: #method<channel.close>(reply-code=406, reply-text=TIMEOUT WAITING FOR ACK, class-id=0, method-id=0)
    - CanalMq batchSize 建议设置不要太大
- 一个服务只能有一个 DefaultSchema 注解，多个注解会导致被覆盖
- tinyint 对应的 Java 类型为 Boolean, 需要指定映射关系
- ERROR c.a.o.c.p.inbound.mysql.rds.RdsBinlogEventParserProxy - dump address cpsbjk-test-1.runlion.priv/192.168.173.123:3306 has an error, retrying. caused by 
  java.io.IOException: Received error packet: errno = 1236, sqlstate = HY000 errmsg = binlog truncated in the middle of event; consider out of disk space on master; the first event 'mysql-bin.000009' at 203148789, the last event read from './mysql-bin.000009' at 205329204, the last byte read from './mysql-bin.000009' at 205329223.
    - master 磁盘满导致 binlog 文件截断，建议定时清理 binlog 文件或者设置失效时间