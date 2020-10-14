package com.fanxuankai.canal.mq.core.listener;

import java.util.Collection;

/**
 * @author fanxuankai
 */
public class SimpleCanalListenerFactory extends AbstractCanalListenerFactory {
    public SimpleCanalListenerFactory(Collection<Object> canalListeners) {
        init(canalListeners);
    }
}
