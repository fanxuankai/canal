package com.fanxuankai.canal.elasticsearch.annotation;

import java.lang.annotation.*;

/**
 * @author fanxuankai
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Indexes {
    Index[] value() default {};
}
