package com.fanxuankai.canal.redis.config;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author fanxuankai
 */
@Data
@ConfigurationProperties(prefix = Constants.PREFIX)
public class CanalRedisProperties {
    @NestedConfigurationProperty
    private CanalConfiguration configuration = new CanalConfiguration();
    @NestedConfigurationProperty
    private CanalRedisConfiguration redisConfiguration = new CanalRedisConfiguration();
}
