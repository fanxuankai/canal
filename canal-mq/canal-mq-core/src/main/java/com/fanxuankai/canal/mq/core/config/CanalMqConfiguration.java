package com.fanxuankai.canal.mq.core.config;

import com.fanxuankai.canal.core.model.EntryWrapper;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class CanalMqConfiguration {

    /**
     * 是否开启 canal 服务
     */
    private Boolean enabled = Boolean.TRUE;

    private Map<String, Map<String, MqConsumerConfig>> consumerConfigMap = Collections.emptyMap();

    /**
     * 全局分组
     */
    private String globalGroup;

    public String getTopic(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(MqConsumerConfig::getTopic).orElse(null);
    }

    public String getGroup(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(MqConsumerConfig::getGroup).orElse(null);
    }

    public Optional<MqConsumerConfig> getConsumerConfig(EntryWrapper entryWrapper) {
        return Optional.ofNullable(consumerConfigMap)
                .map(map -> map.get(entryWrapper.getSchemaName()))
                .map(map -> map.get(entryWrapper.getTableName()));
    }

}
