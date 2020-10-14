package com.fanxuankai.canal.mq.core.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.mq.core.model.MessageInfo;

import java.util.function.Function;

/**
 * @author fanxuankai
 */
public interface MessageFunction extends TopicSupplier, GroupSupplier, Function<EntryWrapper, MessageInfo> {

}
