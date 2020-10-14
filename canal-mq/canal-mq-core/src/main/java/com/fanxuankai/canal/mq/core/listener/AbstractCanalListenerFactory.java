package com.fanxuankai.canal.mq.core.listener;

import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * @author fanxuankai
 */
public abstract class AbstractCanalListenerFactory implements CanalListenerFactory {

    protected final Map<String, Map<String, CanalListenerDefinition>> bySchemaAndTableName = new HashMap<>(16);
    protected final Map<String, Map<String, CanalListenerDefinition>> byGroupAndTopic = new HashMap<>(16);
    protected final Map<String, CanalListenerDefinition> byTopic = new HashMap<>(16);
    protected final Map<Class<?>, CanalListenerDefinition> byEntityClass = new HashMap<>(16);
    protected final List<CanalListenerDefinition> definitions = new ArrayList<>(16);

    @Override
    public List<CanalListenerDefinition> getAllCanalListener() {
        return definitions;
    }

    @Override
    public CanalListenerDefinition getCanalListener(String schema, String tableName) {
        return Optional.ofNullable(bySchemaAndTableName.get(schema))
                .map(o -> o.get(tableName))
                .orElse(null);
    }

    @Override
    public CanalListenerDefinition getCanalListener(Class<?> entityClass) {
        return byEntityClass.get(entityClass);
    }

    @Override
    public CanalListenerDefinition getCanalListenerByTopic(String topic) {
        return byTopic.get(topic);
    }

    @Override
    public CanalListenerDefinition getCanalListenerGroupAndTopic(String group, String topic) {
        return Optional.ofNullable(byGroupAndTopic.get(group))
                .map(o -> o.get(topic))
                .orElse(null);
    }

    protected void init(Collection<Object> canalListeners) {
        if (CollectionUtils.isEmpty(canalListeners)) {
            return;
        }
        for (Object canalListener : canalListeners) {
            MqCanalListenerDefinition definition = new MqCanalListenerDefinition(canalListener);
            definitions.add(definition);
            byEntityClass.put(definition.getEntityClass(), definition);
            bySchemaAndTableName.computeIfAbsent(definition.getSchema(), s -> new HashMap<>(16))
                    .put(definition.getTableName(), definition);
            byTopic.put(definition.getTopic(), definition);
            byGroupAndTopic.computeIfAbsent(definition.getGroup(), s -> new HashMap<>(16))
                    .put(definition.getTopic(), definition);
        }
    }

}
