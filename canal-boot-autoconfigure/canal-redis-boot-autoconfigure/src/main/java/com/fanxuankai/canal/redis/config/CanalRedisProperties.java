package com.fanxuankai.canal.redis.config;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author fanxuankai
 */
@EqualsAndHashCode(callSuper = true)
@Data
@ConfigurationProperties(prefix = CanalRedisProperties.PREFIX)
public class CanalRedisProperties extends CanalRedisConfiguration {
    public static final String PREFIX = Constants.PREFIX + Constants.SEPARATOR + "redis";
    /**
     * 是否开启 canal 服务
     */
    private Boolean enabled = Boolean.TRUE;
    @NestedConfigurationProperty
    private CanalConfiguration configuration = new CanalConfiguration();
}
