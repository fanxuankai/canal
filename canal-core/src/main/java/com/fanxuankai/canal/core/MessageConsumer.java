package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.model.MessageWrapper;

import java.util.function.Consumer;

/**
 * Message 消费者
 *
 * @author fanxuankai
 */
public interface MessageConsumer extends Consumer<MessageWrapper> {

}
