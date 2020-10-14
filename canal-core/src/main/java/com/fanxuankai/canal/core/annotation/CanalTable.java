package com.fanxuankai.canal.core.annotation;

import java.lang.annotation.*;

/**
 * @author fanxuankai
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CanalTable {
    /**
     * @return 数据库名
     */
    String schema() default "";

    /**
     * @return 数据库表名
     */
    String name() default "";
}
