package com.fanxuankai.canal.db.core.util;

/**
 * @author fanxuankai
 */
@FunctionalInterface
public interface ConvertUpdateFunction {
    /**
     * 转 update 脚本
     *
     * @param tableName /
     * @param setSql    /
     * @param idName    /
     * @param idValue   /
     * @return String
     */
    String apply(String tableName, String setSql, String idName, String idValue);
}
