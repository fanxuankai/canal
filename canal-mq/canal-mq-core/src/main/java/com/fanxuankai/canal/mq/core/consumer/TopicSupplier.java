package com.fanxuankai.canal.mq.core.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;

/**
 * @author fanxuankai
 */
public interface TopicSupplier {

    /**
     * Gets a topic.
     *
     * @param entryWrapper data
     * @return a topic
     */
    String getTopic(EntryWrapper entryWrapper);

}
