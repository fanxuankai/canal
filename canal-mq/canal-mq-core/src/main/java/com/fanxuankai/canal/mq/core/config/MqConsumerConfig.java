package com.fanxuankai.canal.mq.core.config;

import com.fanxuankai.canal.core.config.ConsumerConfig;

/**
 * @author fanxuankai
 */
public class MqConsumerConfig extends ConsumerConfig {
    /**
     * 分组
     */
    private String group;
    /**
     * 默认为 schema.table
     */
    private String topic;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}