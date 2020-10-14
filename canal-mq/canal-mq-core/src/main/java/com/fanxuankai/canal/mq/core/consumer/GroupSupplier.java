package com.fanxuankai.canal.mq.core.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;

/**
 * @author fanxuankai
 */
public interface GroupSupplier {

    /**
     * Gets a group.
     *
     * @param entryWrapper data
     * @return a group
     */
    String getGroup(EntryWrapper entryWrapper);

}
