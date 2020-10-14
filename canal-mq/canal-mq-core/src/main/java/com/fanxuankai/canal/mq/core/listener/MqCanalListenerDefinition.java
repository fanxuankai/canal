package com.fanxuankai.canal.mq.core.listener;

import com.fanxuankai.canal.core.annotation.CanalTableCache;
import com.fanxuankai.canal.core.util.Topic;
import com.fanxuankai.canal.mq.core.annotation.CanalListener;
import com.fanxuankai.canal.mq.core.annotation.Delete;
import com.fanxuankai.canal.mq.core.annotation.Insert;
import com.fanxuankai.canal.mq.core.annotation.Update;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author fanxuankai
 */
public class MqCanalListenerDefinition implements CanalListenerDefinition {

    private final CanalListener canalListener;
    private final String group;
    private final String topic;
    private final String schema;
    private final String tableName;
    private Consumer<Object> insertConsumer;
    private BiConsumer<Object, Object> updateConsumer;
    private Consumer<Object> deleteConsumer;

    public MqCanalListenerDefinition(Object object) {
        // 如果该类在事务管理，会被代理，将拿不到注解，需要借助于官方的工具类
        CanalListener canalListener = AnnotationUtils.findAnnotation(object.getClass(), CanalListener.class);
        assert canalListener != null;
        this.canalListener = canalListener;
        this.group = Optional.of(canalListener.group())
                .filter(StringUtils::hasText)
                .map(Topic::custom)
                .orElse(null);
        this.topic = Optional.of(canalListener.topic())
                .filter(StringUtils::hasText)
                .map(Topic::custom)
                .orElse(Topic.of(CanalTableCache.getSchema(canalListener.entityClass()),
                        CanalTableCache.getTableName(canalListener.entityClass())));
        this.schema = CanalTableCache.getSchema(canalListener.entityClass());
        this.tableName = CanalTableCache.getTableName(canalListener.entityClass());
        for (Method method : object.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            if (AnnotationUtils.findAnnotation(method, Insert.class) != null) {
                this.insertConsumer = o -> {
                    try {
                        method.invoke(object, o);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                };
            } else if (AnnotationUtils.findAnnotation(method, Update.class) != null) {
                this.updateConsumer = (o, o2) -> {
                    try {
                        method.invoke(object, o, o2);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                };
            } else if (AnnotationUtils.findAnnotation(method, Delete.class) != null) {
                this.deleteConsumer = o -> {
                    try {
                        method.invoke(object, o);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                };
            }
        }
    }

    @Override
    public String getGroup() {
        return group;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getWaitRateSeconds() {
        return canalListener.waitRateSeconds();
    }

    @Override
    public int getWaitMaxSeconds() {
        return canalListener.waitMaxSeconds();
    }

    @Nullable
    @Override
    public String getSchema() {
        return schema;
    }

    @Nullable
    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Object> getEntityClass() {
        return (Class<Object>) canalListener.entityClass();
    }

    @Nullable
    @Override
    public Consumer<Object> getInsertConsumer() {
        return insertConsumer;
    }

    @Nullable
    @Override
    public Consumer<Object> getDeleteConsumer() {
        return deleteConsumer;
    }

    @Nullable
    @Override
    public BiConsumer<Object, Object> getUpdateConsumer() {
        return updateConsumer;
    }
}
