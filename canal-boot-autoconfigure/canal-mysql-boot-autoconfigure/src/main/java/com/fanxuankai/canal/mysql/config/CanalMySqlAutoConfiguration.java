package com.fanxuankai.canal.mysql.config;

import com.fanxuankai.canal.mysql.CanalMySqlWorker;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author fanxuankai
 */
@EnableConfigurationProperties(CanalMySqlProperties.class)
@ConditionalOnProperty(prefix = CanalMySqlProperties.PREFIX, name = "enabled", havingValue = "true")
public class CanalMySqlAutoConfiguration {

    @Bean
    public CanalMySqlWorker canalMySqlWorker(CanalMySqlProperties canalMySqlProperties, JdbcTemplate jdbcTemplate) {
        return CanalMySqlWorker.newCanalWorker(canalMySqlProperties.getConfiguration(), canalMySqlProperties,
                jdbcTemplate);
    }

}
