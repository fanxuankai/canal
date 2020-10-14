package com.fanxuankai.canal.mq.core.listener;

import java.util.List;

/**
 * Canal 监听器工厂
 *
 * @author fanxuankai
 */
public interface CanalListenerFactory {

    /**
     * 获得所有监听器
     *
     * @return 如果没有则返回 emptyList
     */
    List<CanalListenerDefinition> getAllCanalListener();

    /**
     * 根据数据库和表名获得监听器
     *
     * @param schema    数据库
     * @param tableName 表名
     * @return Canal 监听器
     */
    CanalListenerDefinition getCanalListener(String schema, String tableName);

    /**
     * 根据数据库和表名获得监听器
     *
     * @param entityClass 实体类类型
     * @return Canal 监听器
     */
    CanalListenerDefinition getCanalListener(Class<?> entityClass);

    /**
     * 根据主题名获取监听器
     *
     * @param topic 主题
     * @return Canal 监听器
     */
    CanalListenerDefinition getCanalListenerByTopic(String topic);

    /**
     * 根据主题名获取监听器
     *
     * @param group 分组
     * @param topic 主题
     * @return Canal 监听器
     */
    CanalListenerDefinition getCanalListenerGroupAndTopic(String group, String topic);

}
