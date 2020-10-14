package com.fanxuankai.canal.mq.core.config;

import com.fanxuankai.canal.core.config.ConsumerConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author fanxuankai
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MqConsumerConfig extends ConsumerConfig {
    /**
     * 分组
     */
    private String group;
    /**
     * 默认为 schema.table
     */
    private String topic;
}