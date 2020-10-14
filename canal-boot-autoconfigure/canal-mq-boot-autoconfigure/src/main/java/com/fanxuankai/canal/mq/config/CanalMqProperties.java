package com.fanxuankai.canal.mq.config;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author fanxuankai
 */
@Data
@ConfigurationProperties(prefix = Constants.PREFIX)
public class CanalMqProperties {

    @NestedConfigurationProperty
    private CanalConfiguration configuration = new CanalConfiguration();
    @NestedConfigurationProperty
    private CanalMqConfiguration mqConfiguration = new CanalMqConfiguration();

}
