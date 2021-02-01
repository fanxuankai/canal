package com.fanxuankai.canal.test;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.clickhouse.CanalClickhouseWorker;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.config.DbConsumerConfig;
import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

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
        consumerConfigMap.put("clickhouse_demo", consumerConfigValue);
        CanalDbConfiguration canalDbConfiguration = new CanalDbConfiguration();
        canalDbConfiguration.setConsumerConfigMap(consumerConfigMap);
        canalDbConfiguration.setSchemaName("default");

        CanalConfiguration canalConfiguration = new CanalConfiguration();
        canalConfiguration.setInstance("canalClickhouseExample");
        canalConfiguration.setFilter("clickhouse_demo.user,clickhouse_demo.dept,clickhouse_demo.post");
        canalConfiguration.setShowEventLog(true);
        CanalConfiguration.MergeEntry mergeEntry = new CanalConfiguration.MergeEntry();
        mergeEntry.setMerge(true);
        Map<CanalEntry.EventType, Integer> maxRowDataSize = Maps.newHashMap();
        maxRowDataSize.put(INSERT, 1000);
        maxRowDataSize.put(UPDATE, 10000);
        maxRowDataSize.put(DELETE, 10000);
        mergeEntry.setMaxRowDataSize(maxRowDataSize);
        canalConfiguration.setMergeEntry(mergeEntry);
        canalConfiguration.setBatchSize(10000);
        canalConfiguration.setParallel(true);
        canalConfiguration.setSkip(false);

        CanalWorker canalWorker = CanalClickhouseWorker.newCanalWorker(canalConfiguration, canalDbConfiguration,
                new JdbcTemplate(dataSource));
        canalWorker.getCanalWorkConfiguration()
                .setRedisTemplate(RedisTemplates.newRedisTemplate());
        canalWorker.start();
    }
}