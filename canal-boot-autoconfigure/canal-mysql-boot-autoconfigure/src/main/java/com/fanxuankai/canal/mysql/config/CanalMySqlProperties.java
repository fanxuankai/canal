package com.fanxuankai.canal.mysql.config;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author fanxuankai
 */
@EqualsAndHashCode(callSuper = true)
@Data
@ConfigurationProperties(prefix = CanalMySqlProperties.PREFIX)
public class CanalMySqlProperties extends CanalDbConfiguration {
    public static final String PREFIX = Constants.PREFIX + Constants.SEPARATOR + "mysql";
    /**
     * 是否开启 canal 服务
     */
    private Boolean enabled = Boolean.TRUE;
    @NestedConfigurationProperty
    private CanalConfiguration configuration = new CanalConfiguration();
}
