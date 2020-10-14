package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.core.annotation.CanalTableCache;
import com.fanxuankai.canal.elasticsearch.annotation.Index;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.lang.Nullable;

/**
 * @author fanxuankai
 */
public abstract class AbstractIndexDefinition implements IndexDefinition {
    protected final Class<?> domainClass;
    protected final Index index;
    protected final String schema;
    protected final String tableName;
    protected final Document document;

    public AbstractIndexDefinition(Class<?> domainClass, Index index) {
        this.domainClass = domainClass;
        this.index = index;
        this.schema = CanalTableCache.getSchema(domainClass);
        this.tableName = CanalTableCache.getTableName(domainClass);
        assert index != null;
        this.document = AnnotationUtils.findAnnotation(index.documentClass(), Document.class);
    }

    @Override
    public String getIndexName() {
        return document.indexName();
    }

    @Override
    public String getType() {
        return document.type();
    }

    @Nullable
    @Override
    public String getSchema() {
        return schema;
    }

    @Nullable
    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Class<?> getEntityClass() {
        return domainClass;
    }

    @Override
    public Class<?> getDocumentClass() {
        return index.documentClass();
    }

}
