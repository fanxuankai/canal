package com.fanxuankai.canal.test;

import com.fanxuankai.canal.clickhouse.CanalClickhouseWorker;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.config.DbConsumerConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fanxuankai
 */
public class CanalClickhouseDemo {
    public static void main(String[] args) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:clickhouse://192.168.173.201:8123");
        hikariConfig.setDriverClassName("ru.yandex.clickhouse.ClickHouseDriver");
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);

        Map<String, Map<String, DbConsumerConfig>> consumerConfigMap = new HashMap<>(16);
        Map<String, DbConsumerConfig> consumerConfigValue = new HashMap<>(16);
        DbConsumerConfig dbConsumerConfig = new DbConsumerConfig();
        dbConsumerConfig.setIgnoreChangeColumns(Collections.singletonList("date"));
        dbConsumerConfig.setSchemaName("default");
        consumerConfigValue.put("dept", dbConsumerConfig);
        consumerConfigValue.put("post", dbConsumerConfig);
        consumerConfigValue.put("user", dbConsumerConfig);
        consumerConfigMap.put("clickhouse_demo", consumerConfigValue);
        CanalDbConfiguration canalDbConfiguration = new CanalDbConfiguration();
        canalDbConfiguration.setConsumerConfigMap(consumerConfigMap);

        CanalConfiguration canalConfiguration = new CanalConfiguration();
        canalConfiguration.setInstance("canalClickhouseExample");
        canalConfiguration.setFilter("clickhouse_demo.user,clickhouse_demo.dept,clickhouse_demo.post");
        canalConfiguration.setShowEventLog(true);
        CanalConfiguration.MergeEntry mergeEntry = new CanalConfiguration.MergeEntry();
        mergeEntry.setMerge(true);
        canalConfiguration.setMergeEntry(mergeEntry);
        canalConfiguration.setBatchSize(500);
        canalConfiguration.setParallel(true);

        CanalWorker canalWorker = CanalClickhouseWorker.newCanalWorker(canalConfiguration, canalDbConfiguration,
                new JdbcTemplate(dataSource));
        canalWorker.getCanalWorkConfiguration()
                .setRedisTemplate(RedisTemplates.newRedisTemplate());
        canalWorker.start();
    }
}