package com.fanxuankai.canal.mq.core.annotation;

import java.lang.annotation.*;

/**
 * canal 监听器
 *
 * @author fanxuankai
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CanalListener {

    /**
     * 分组
     *
     * @return String
     */
    String group() default "";

    /**
     * 主题
     *
     * @return String
     */
    String topic() default "";

    /**
     * 实体类
     *
     * @return String
     */
    Class<?> entityClass();

    /**
     * 线程等待递增时长 s, 仅 runlion-xxl-mq 生效
     *
     * @return int
     */
    int waitRateSeconds() default 5;

    /**
     * 线程等待最大时长 s, 仅 runlion-xxl-mq 生效
     *
     * @return int
     */
    int waitMaxSeconds() default 30;
}
