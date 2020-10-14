package com.fanxuankai.canal.redis.annotation;

import java.lang.annotation.*;

/**
 * Redis 配置
 *
 * @author fanxuankai
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CanalRedis {
    /**
     * @return hashKey
     */
    String key();
}
