package com.fanxuankai.canal.test;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.redis.CanalRedisWorker;
import com.fanxuankai.canal.redis.config.CanalRedisConfiguration;

/**
 * @author fanxuankai
 */
public class CanalRedisDemo {
    public static void main(String[] args) {
        CanalRedisWorker.newCanalWorker(new CanalConfiguration()
                        .setInstance("canalRedisExample")
                        .setShowEventLog(true)
                        .setShowEntryLog(true),
                new CanalRedisConfiguration(), RedisTemplates.newRedisTemplate())
                .start();
    }
}