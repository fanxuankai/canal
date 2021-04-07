package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.core.util.ApplicationContextHolder;
import com.fanxuankai.canal.elasticsearch.annotation.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * @author fanxuankai
 */
public class SimpleIndexDefinition extends AbstractIndexDefinition {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleIndexDefinition.class);
    private DocumentFunction<Object, Object> documentFunction;

    public SimpleIndexDefinition(Class<?> domainClass, Index index) {
        super(domainClass, index);
    }

    public SimpleIndexDefinition(Class<?> domainClass, Index index, DocumentFunction<Object, Object> documentFunction) {
        super(domainClass, index);
        this.documentFunction = documentFunction;
    }

    @Override
    public DocumentFunction<Object, Object> getDocumentFunction() {
        return getOrInit();
    }

    @SuppressWarnings("unchecked")
    private DocumentFunction<Object, Object> getOrInit() {
        if (documentFunction != null) {
            return documentFunction;
        }
        ApplicationContext applicationContext = ApplicationContextHolder.getApplicationContext();
        if (applicationContext != null) {
            try {
                documentFunction = applicationContext.getBean(index.documentFunctionClass());
            } catch (Exception e) {
                LOGGER.debug("从 ApplicationContext 获取失败", e);
            }
        }
        if (documentFunction == null) {
            try {
                documentFunction = index.documentFunctionClass().getDeclaredConstructor().newInstance();
            } catch (Exception e1) {
                throw new IllegalArgumentException();
            }
        }
        return documentFunction;
    }

}
