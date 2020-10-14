package com.fanxuankai.canal.mq.core.listener;

import org.springframework.lang.Nullable;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Canal 监听器定义
 *
 * @author fanxuankai
 */
public interface CanalListenerDefinition {

    /**
     * 主题
     *
     * @return String
     */
    String getTopic();

    /**
     * 数据库名
     *
     * @return 为空时取全局数据库名
     */
    @Nullable
    String getSchema();

    /**
     * 表名
     *
     * @return 为空时取实体类类名，如果实体类有 @Table、@TableName 等注解，就取注解上的表名
     */
    @Nullable
    String getTableName();

    /**
     * 表对应的实体类类型
     *
     * @return domain class
     */
    Class<Object> getEntityClass();

    /**
     * 新增事件消费者
     *
     * @return 消费者接受新增数据
     */
    @Nullable
    Consumer<Object> getInsertConsumer();

    /**
     * 删除事件消费者
     *
     * @return 消费者接受删除数据
     */
    @Nullable
    Consumer<Object> getDeleteConsumer();

    /**
     * 修改事件消费者
     *
     * @return 消费者接受修改前和修改后的数据
     */
    @Nullable
    BiConsumer<Object, Object> getUpdateConsumer();

    /**
     * 分组
     *
     * @return string
     */
    @Nullable
    String getGroup();

    /**
     * 线程等待递增时长 s, 仅 runlion-xxl-mq 生效
     *
     * @return int
     */
    int getWaitRateSeconds();

    /**
     * 线程等待最大时长 s, 仅 runlion-xxl-mq 生效
     *
     * @return int
     */
    int getWaitMaxSeconds();
}
