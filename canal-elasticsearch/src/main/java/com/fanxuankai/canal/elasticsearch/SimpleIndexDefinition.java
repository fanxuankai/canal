package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.core.util.ApplicationContextHolder;
import com.fanxuankai.canal.elasticsearch.annotation.Index;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

/**
 * @author fanxuankai
 */
@Slf4j
public class SimpleIndexDefinition extends AbstractIndexDefinition {

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
                log.debug("从 ApplicationContext 获取失败", e);
                try {
                    documentFunction = index.documentFunctionClass().getDeclaredConstructor().newInstance();
                } catch (Exception e1) {
                    throw new IllegalArgumentException();
                }
            }
        }
        return documentFunction;
    }

}
