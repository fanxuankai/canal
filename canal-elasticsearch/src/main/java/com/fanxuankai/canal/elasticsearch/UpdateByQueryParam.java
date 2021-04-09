package com.fanxuankai.canal.elasticsearch;

/**
 * @author fanxuankai
 */
public class UpdateByQueryParam {
    private IndexDefinition indexDefinition;
    private UpdateByQuery updateByQuery;

    public UpdateByQueryParam() {
    }

    public UpdateByQueryParam(IndexDefinition indexDefinition, UpdateByQuery updateByQuery) {
        this.indexDefinition = indexDefinition;
        this.updateByQuery = updateByQuery;
    }

    public IndexDefinition getIndexDefinition() {
        return indexDefinition;
    }

    public void setIndexDefinition(IndexDefinition indexDefinition) {
        this.indexDefinition = indexDefinition;
    }

    public UpdateByQuery getUpdateByQuery() {
        return updateByQuery;
    }

    public void setUpdateByQuery(UpdateByQuery updateByQuery) {
        this.updateByQuery = updateByQuery;
    }
}