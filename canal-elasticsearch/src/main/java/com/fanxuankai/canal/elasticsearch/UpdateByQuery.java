package com.fanxuankai.canal.elasticsearch;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * 适用于 OneToMany, 用于批量修改数据, 非 OneToMany 不建议使用该方式
 *
 * @author fanxuankai
 */
public class UpdateByQuery {
    private QueryBuilder queryBuilder;
    private Map<String, Object> data;

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public void setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}