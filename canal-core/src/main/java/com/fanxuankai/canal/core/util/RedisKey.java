package com.fanxuankai.canal.core.util;

import cn.hutool.core.text.StrPool;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Redis 工具类
 *
 * @author fanxuankai
 */
public class RedisKey {

    /**
     * 前缀
     */
    protected static final String PREFIX = "canal" + StrPool.COLON + "canalDbCache";

    /**
     * 生成 key
     *
     * @param schema 数据库名
     * @param table  表名
     * @return 生成默认的 key
     */
    public static String of(String schema, String table) {
        return of(schema, table, null);
    }

    /**
     * 生成 key
     *
     * @param schema 数据库名
     * @param table  表名
     * @param suffix 后缀
     * @return 生成默认的 key
     */
    public static String of(String schema, String table, String suffix) {
        String key = PREFIX + StrPool.COLON + schema + StrPool.COLON + table;
        if (suffix != null && !schema.isEmpty()) {
            return key + StrPool.COLON + suffix;
        }
        return key;
    }

    /**
     * 生成 key
     *
     * @param key    自定义的key
     * @param suffix 后缀
     * @return 生成自定义的 key
     */
    public static String withSuffix(String key, String suffix) {
        return key + StrPool.COLON + suffix;
    }

    /**
     * 生成 key
     *
     * @param prefix 后缀
     * @param custom 自定义
     * @return 生成自定义的 key
     */
    public static String withPrefix(String prefix, String custom) {
        return prefix + StrPool.COLON + custom;
    }

    /**
     * 生成 key 的后缀
     *
     * @param columnList 数据库列名
     * @return column0:column1:column2
     */
    public static String suffix(List<String> columnList) {
        return String.join(StrPool.COLON, columnList);
    }

    /**
     * 生成 hashKey
     *
     * @param columnList 数据库列名
     * @param columnMap  数据库列键值对
     * @return (columnValue0):(columnValue1):(columnValue2)
     */
    public static String hashKey(List<String> columnList, Map<String, Object> columnMap) {
        return columnList.stream()
                .map(columnMap::get)
                .map(s -> "(" + s + ")")
                .collect(Collectors.joining(StrPool.COLON));
    }
}
