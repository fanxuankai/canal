package com.fanxuankai.canal.mysql.config;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author fanxuankai
 */
@Data
@ConfigurationProperties(prefix = Constants.PREFIX)
public class CanalMySqlProperties {
    @NestedConfigurationProperty
    private CanalConfiguration configuration = new CanalConfiguration();
    @NestedConfigurationProperty
    private CanalDbConfiguration dbConfiguration = new CanalDbConfiguration();
}
