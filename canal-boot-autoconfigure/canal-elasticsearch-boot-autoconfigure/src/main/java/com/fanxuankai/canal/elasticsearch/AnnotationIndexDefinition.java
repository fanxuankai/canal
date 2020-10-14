package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.boot.commons.util.ApplicationContexts;
import com.fanxuankai.canal.elasticsearch.annotation.Index;

/**
 * @author fanxuankai
 */
public class AnnotationIndexDefinition extends AbstractIndexDefinition {

    public AnnotationIndexDefinition(Class<?> domainClass, Index index) {
        super(domainClass, index);
    }

    @Override
    @SuppressWarnings("unchecked")
    public DocumentFunction<Object, Object> getDocumentFunction() {
        return ApplicationContexts.getBean(index.documentFunctionClass());
    }
}
