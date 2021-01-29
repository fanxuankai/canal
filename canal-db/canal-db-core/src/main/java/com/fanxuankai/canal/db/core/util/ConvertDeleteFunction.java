package com.fanxuankai.canal.db.core.util;

/**
 * @author fanxuankai
 */
@FunctionalInterface
public interface ConvertDeleteFunction {
    /**
     * 转 delete 脚本
     *
     * @param tableName /
     * @param idName    /
     * @param ids       /
     * @return String
     */
    String apply(String tableName, String idName, String ids);
}
