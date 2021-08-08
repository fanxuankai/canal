package com.fanxuankai.canal.core.util;

import cn.hutool.core.text.StrPool;

/**
 * 主题
 *
 * @author fanxuankai
 */
public class Topic {

    protected static final String CANAL_2_MQ = "canal2Mq";

    public static String of(String schema, String table) {
        return CANAL_2_MQ + StrPool.UNDERLINE + schema + StrPool.UNDERLINE + table;
    }

    public static String of(String schema, String table, String eventType) {
        return CANAL_2_MQ + StrPool.UNDERLINE + schema + StrPool.UNDERLINE + table + StrPool.UNDERLINE + eventType;
    }

    public static String custom(String queue, String eventType) {
        return CANAL_2_MQ + StrPool.UNDERLINE + queue + StrPool.UNDERLINE + eventType;
    }

    public static String custom(String queue) {
        return CANAL_2_MQ + StrPool.UNDERLINE + queue;
    }

    public static String customWithoutPrefix(String queue, String eventType) {
        return queue + StrPool.UNDERLINE + eventType;
    }
}
