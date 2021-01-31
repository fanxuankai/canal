package com.fanxuankai.canal.test;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.redis.CanalRedisWorker;
import com.fanxuankai.canal.redis.config.CanalRedisConfiguration;

/**
 * @author fanxuankai
 */
public class CanalRedisDemo {
    public static void main(String[] args) {
        CanalConfiguration canalConfiguration = new CanalConfiguration();
        CanalConfiguration.Cluster cluster = new CanalConfiguration.Cluster();
        cluster.setNodes("localhost:3181,localhost:3182,localhost:3183");
        canalConfiguration.setCluster(cluster);
        canalConfiguration.setInstance("canalRedisExample");
        canalConfiguration.setFilter("canal_client_example.t_user");
        canalConfiguration.setShowEventLog(true);
        canalConfiguration.setShowEntryLog(false);
        canalConfiguration.setBatchSize(10000);
        canalConfiguration.setParallel(true);
        CanalConfiguration.MergeEntry mergeEntry = new CanalConfiguration.MergeEntry();
        mergeEntry.setMerge(true);
        canalConfiguration.setMergeEntry(mergeEntry);
        CanalRedisWorker.newCanalWorker(canalConfiguration, new CanalRedisConfiguration(),
                RedisTemplates.newRedisTemplate())
                .start();
    }
}