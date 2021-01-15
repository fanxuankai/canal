package com.fanxuankai.canal.core.util;

import static com.fanxuankai.canal.core.constants.Constants.SEPARATOR2;

/**
 * 主题
 *
 * @author fanxuankai
 */
public class Topic {

    protected static final String CANAL_2_MQ = "canal2Mq";

    public static String of(String schema, String table) {
        return CANAL_2_MQ + SEPARATOR2 + schema + SEPARATOR2 + table;
    }

    public static String of(String schema, String table, String eventType) {
        return CANAL_2_MQ + SEPARATOR2 + schema + SEPARATOR2 + table + SEPARATOR2 + eventType;
    }

    public static String custom(String queue, String eventType) {
        return CANAL_2_MQ + SEPARATOR2 + queue + SEPARATOR2 + eventType;
    }

    public static String custom(String queue) {
        return CANAL_2_MQ + SEPARATOR2 + queue;
    }

    public static String customWithoutPrefix(String queue, String eventType) {
        return queue + SEPARATOR2 + eventType;
    }
}
