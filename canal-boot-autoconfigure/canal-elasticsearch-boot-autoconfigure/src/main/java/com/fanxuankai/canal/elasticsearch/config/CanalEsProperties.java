package com.fanxuankai.canal.elasticsearch.config;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author fanxuankai
 */
@ConfigurationProperties(prefix = Constants.PREFIX)
@Data
public class CanalEsProperties {
    @NestedConfigurationProperty
    private CanalConfiguration configuration = new CanalConfiguration();
    @NestedConfigurationProperty
    private CanalElasticsearchConfiguration elasticsearchConfiguration = new CanalElasticsearchConfiguration();
}
