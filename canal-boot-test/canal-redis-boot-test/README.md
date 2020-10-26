## Getting started

- 添加 maven 依赖
```xml
<dependency>
    <groupId>com.fanxuankai.canal</groupId>
    <artifactId>canal-redis-boot-starter</artifactId>
    <version>${latestVersion}</version>
</dependency>
```
- 配置
```yml
## application.yml
#canal:
  #redis:
    # 消费配置
    #consumer-config-map:
      # 数据库
      #schema:
        # 表
        #table: 
          # 默认为 schema.table
          #key:
          # id 是否作为 hashKey, 默认为 true
          #id-as-hash-key: true
          # hash key 增加 uniqueKey 后缀, 作为 hash 的集合名, 以 uniqueKey 的值作为 hash 的 field
          #unique-keys:
          # hash key 增加 combineKeys 后缀, 作为 hash 的集合名, 以 combineKeys 的值作为 hash 的 field
          #combine-keys:
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
[canal-redis-boot-test](https://github.com/fanxuankai/canal/tree/main/canal-boot-test/canal-redis-boot-test)