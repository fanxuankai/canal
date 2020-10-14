package com.fanxuankai.canal.core.util;

import static com.fanxuankai.canal.core.constants.Constants.SEPARATOR;

/**
 * 主题
 *
 * @author fanxuankai
 */
public class Topic {

    protected static final String CANAL_2_MQ = "canal2Mq";

    public static String of(String schema, String table) {
        return CANAL_2_MQ + SEPARATOR + schema + SEPARATOR + table;
    }

    public static String of(String schema, String table, String eventType) {
        return CANAL_2_MQ + SEPARATOR + schema + SEPARATOR + table + SEPARATOR + eventType;
    }

    public static String custom(String queue, String eventType) {
        return CANAL_2_MQ + SEPARATOR + queue + SEPARATOR + eventType;
    }

    public static String custom(String queue) {
        return CANAL_2_MQ + SEPARATOR + queue;
    }
}
