package com.fanxuankai.canal.mq.config;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author fanxuankai
 */
@EqualsAndHashCode(callSuper = true)
@Data
@ConfigurationProperties(prefix = CanalMqProperties.PREFIX)
public class CanalMqProperties extends CanalMqConfiguration {
    public static final String PREFIX = Constants.PREFIX + Constants.SEPARATOR + "mq";
    /**
     * 是否开启 canal 服务
     */
    private Boolean enabled = Boolean.TRUE;
    @NestedConfigurationProperty
    private CanalConfiguration configuration = new CanalConfiguration();

}
