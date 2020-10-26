## Getting started
- 添加 maven 依赖
```xml
<dependency>
    <groupId>com.fanxuankai.canal</groupId>
    <artifactId>canal-elasticsearch-boot-starter</artifactId>
    <version>${latestVersion}</version>
</dependency>
```
- 配置
```
## application.yml
spring:
  data:
    elasticsearch:
      cluster-nodes: 127.0.0.1:9300
#canal:
  #elasticsearch:
    # 消费配置
    #consumer-config-map:
      # 数据库
      #schema:
        # 表
        #table:
          # 默认为 schema.table
          #indexName:
          # 数据过滤
          #filter:
            # Aviator 表达式, 表达式必须返回 true or false
            #aviator-expression:
            # 字段值发生变化
            #updated-columns:
            # 任一字段发生变化即可
            #any-updated:
            # 事件类型过滤, 默认为增、删、改、删表
            #event-types:
          # 逻辑删除字段
          #logic-delete-field: deleted
```

## Example
[canal-elasticsearch-boot-test](https://github.com/fanxuankai/canal/tree/main/canal-boot-test/canal-elasticsearch-boot-test)