## Getting started

- 添加 maven 依赖
```xml
<dependency>
    <groupId>com.fanxuankai.canal</groupId>
    <artifactId>canal-mysql-boot-starter</artifactId>
    <version>${latestVersion}</version>
</dependency>
```
- 配置
```yml
## application.yml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/canal_client_example?autoReconnect=true&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=Asia/Shanghai
    username: root
    password: HzB!OPxxE$5CwJIZ
    driver-class-name: com.mysql.cj.jdbc.Driver
#canal:
  #mysql:
    # 消费配置
    #consumer-config-map:
      # 数据库
      #schema:
      # 表
      #table:
        # 表名
        #table-name:
        # 包含的列
        #include-columns:
        # 排除的列
        #exclude-columns:
        # key: 源字段名 value: 目标字段名
        #column-map:
        # 目标数据库设默认值
        #default-values:
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
[canal-mysql-boot-test](https://github.com/fanxuankai/canal/tree/main/canal-boot-test/canal-mysql-boot-test)