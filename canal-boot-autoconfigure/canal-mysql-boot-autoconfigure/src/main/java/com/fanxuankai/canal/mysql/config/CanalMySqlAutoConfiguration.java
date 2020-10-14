package com.fanxuankai.canal.mysql.config;

import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.mysql.CanalMySqlWorker;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author fanxuankai
 */
@EnableConfigurationProperties(CanalMySqlProperties.class)
@ConditionalOnProperty(prefix = Constants.PREFIX + ".db-configuration", name = "enabled", havingValue = "true")
public class CanalMySqlAutoConfiguration {

    @Bean
    public CanalMySqlWorker canalMySqlWorker(CanalMySqlProperties canalMySqlProperties, JdbcTemplate jdbcTemplate) {
        return CanalMySqlWorker.newCanalWorker(canalMySqlProperties.getConfiguration(),
                canalMySqlProperties.getDbConfiguration(), jdbcTemplate);
    }

}
