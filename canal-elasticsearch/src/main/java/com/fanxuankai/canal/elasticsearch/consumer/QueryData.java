package com.fanxuankai.canal.elasticsearch.consumer;

/**
 * @author fanxuankai
 */
public class QueryData {
    /**
     * 索引名,新版本增删改需要 IndexCoordinates
     * 也能使用 elasticsearchRestTemplate.getIndexCoordinatesFor() 获得
     */
    private String indexName;
    /**
     * Query 对象
     */
    private Object query;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Object getQuery() {
        return query;
    }

    public void setQuery(Object query) {
        this.query = query;
    }
}