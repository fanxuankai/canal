package com.fanxuankai.canal.elasticsearch;

import org.springframework.lang.Nullable;

/**
 * 索引构建定义
 *
 * @author fanxuankai
 */
public interface IndexDefinition {

    /**
     * 索引名
     *
     * @return the index name
     */
    String getIndexName();

    /**
     * 数据库名
     *
     * @return 为空时取全局数据库名
     */
    @Nullable
    String getSchema();

    /**
     * 表名
     *
     * @return 为空时取实体类类名，如果实体类有 @Table、@TableName 等注解，就取注解上的表名
     */
    @Nullable
    String getTableName();

    /**
     * 表对应的实体类类型
     *
     * @return domain class
     */
    Class<?> getEntityClass();

    /**
     * doc 类型
     *
     * @return the doc class
     */
    Class<?> getDocumentClass();

    /**
     * 构建文档函数类
     *
     * @return the doc function class
     */
    DocumentFunction<Object, Object> getDocumentFunction();
}
