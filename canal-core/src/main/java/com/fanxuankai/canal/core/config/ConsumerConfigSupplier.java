package com.fanxuankai.canal.core.config;

import com.fanxuankai.canal.core.model.EntryWrapper;

/**
 * @author fanxuankai
 */
public interface ConsumerConfigSupplier {

    /**
     * 获取消费配置
     *
     * @param entryWrapper 数据
     * @return ConsumerConfig
     */
    ConsumerConfig getConsumerConfig(EntryWrapper entryWrapper);

}
