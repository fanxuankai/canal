package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.elasticsearch.annotation.Index;

/**
 * @author fanxuankai
 */
public class SimpleIndexDefinition extends AbstractIndexDefinition {

    private final DocumentFunction<Object, Object> documentFunction;

    @SuppressWarnings("unchecked")
    public SimpleIndexDefinition(Class<?> domainClass, Index index) {
        super(domainClass, index);
        try {
            this.documentFunction = index.documentFunctionClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    public SimpleIndexDefinition(Class<?> domainClass, Index index, DocumentFunction<Object, Object> documentFunction) {
        super(domainClass, index);
        this.documentFunction = documentFunction;
    }

    @Override
    public DocumentFunction<Object, Object> getDocumentFunction() {
        return documentFunction;
    }

}
