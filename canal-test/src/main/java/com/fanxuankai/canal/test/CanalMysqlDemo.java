package com.fanxuankai.canal.test;

import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.config.DbConsumerConfig;
import com.fanxuankai.canal.mysql.CanalMySqlWorker;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fanxuankai
 */
public class CanalMysqlDemo {
    public static void main(String[] args) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:mysql://localhost:3306/canal_db?autoReconnect=true&useUnicode=true" +
                "&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=Asia/Shanghai");
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariConfig.setUsername("root");
        hikariConfig.setPassword("HzB!OPxxE$5CwJIZ");
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);

        Map<String, Map<String, DbConsumerConfig>> consumerConfigMap = new HashMap<>(16);
        Map<String, DbConsumerConfig> consumerConfigValue = new HashMap<>(16);
        Map<String, String> userColumnMap = new HashMap<>(16);
        Map<String, String> userDefaultValues = new HashMap<>(16);
        userDefaultValues.put("deleted", "0");
        userColumnMap.put("version", "version1");
        consumerConfigValue.put("t_user", new DbConsumerConfig()
                .setDefaultValues(userDefaultValues)
                .setExcludeColumns(Collections.singletonList("status"))
                .setColumnMap(userColumnMap));
        consumerConfigMap.put("canal_client_example", consumerConfigValue);

        CanalWorker canalWorker = CanalMySqlWorker.newCanalWorker(new CanalConfiguration()
                        .setInstance("canalMysqlExample")
                        .setShowEventLog(true)
                        .setShowEntryLog(true),
                new CanalDbConfiguration()
                        .setConsumerConfigMap(consumerConfigMap),
                new JdbcTemplate(dataSource));
        canalWorker.getCanalWorkConfiguration()
                .setRedisTemplate(RedisTemplates.newRedisTemplate());
        canalWorker.start();
    }
}