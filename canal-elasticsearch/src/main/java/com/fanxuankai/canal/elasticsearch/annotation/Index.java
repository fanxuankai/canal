package com.fanxuankai.canal.elasticsearch.annotation;

import com.fanxuankai.canal.elasticsearch.DocumentFunction;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author fanxuankai
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {

    /**
     * doc 类型
     *
     * @return the doc class
     */
    Class<?> documentClass();

    /**
     * 构建文档函数类
     *
     * @return the doc function class
     */
    @SuppressWarnings("rawtypes")
    Class<? extends DocumentFunction> documentFunctionClass();
}
