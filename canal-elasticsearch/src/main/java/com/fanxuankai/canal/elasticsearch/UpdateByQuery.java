package com.fanxuankai.canal.elasticsearch;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

/**
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