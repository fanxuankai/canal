package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.model.EntryWrapper;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Entry 消费者
 *
 * @author fanxuankai
 */
public interface EntryConsumer<R> extends Function<EntryWrapper, R>, Consumer<R> {

    /**
     * 是否可以过滤.
     * 通常情况下是可以过滤的, 数据库的删除事件不可过滤必须消费
     *
     * @return boolean
     */
    default boolean filterable() {
        return true;
    }

}
