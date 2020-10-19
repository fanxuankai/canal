package com.fanxuankai.canal.elasticsearch.config;

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
@ConfigurationProperties(prefix = CanalEsProperties.PREFIX)
@Data
public class CanalEsProperties extends CanalElasticsearchConfiguration {
    public static final String PREFIX = Constants.PREFIX + Constants.SEPARATOR + "elasticsearch";
    /**
     * 是否开启 canal 服务
     */
    private Boolean enabled = Boolean.TRUE;
    @NestedConfigurationProperty
    private CanalConfiguration configuration = new CanalConfiguration();
}
