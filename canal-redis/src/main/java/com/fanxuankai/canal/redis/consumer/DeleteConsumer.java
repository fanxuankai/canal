package com.fanxuankai.canal.redis.consumer;

import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.RedisKey;
import com.fanxuankai.canal.core.util.SqlTypeConverter;
import com.fanxuankai.canal.redis.config.CanalRedisConfiguration;
import com.google.common.collect.Maps;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 删除事件消费者
 *
 * @author fanxuankai
 */
public class DeleteConsumer extends AbstractRedisConsumer<Map<String, List<String>>> {

    public DeleteConsumer(CanalRedisConfiguration canalRedisConfiguration,
                          RedisTemplate<String, Object> redisTemplate) {
        super(canalRedisConfiguration, redisTemplate);
    }

    @Override
    public Map<String, List<String>> apply(EntryWrapper entryWrapper) {
        List<String> uniqueKeys = canalRedisConfiguration.getUniqueKeys(entryWrapper);
        boolean idAsHashKey = canalRedisConfiguration.isIdAsHashKey(entryWrapper);
        List<List<String>> combineKeys = canalRedisConfiguration.getCombineKeys(entryWrapper);
        String key = keyOf(entryWrapper);
        Map<String, List<String>> hash = Maps.newHashMap();
        String schemaName = entryWrapper.getSchemaName();
        String tableName = entryWrapper.getTableName();
        ConsumerConfig consumerConfig = getConsumerConfig(entryWrapper);
        entryWrapper.getAllRowDataList()
                .forEach(rowData -> {
                    rowData.getBeforeColumnsList()
                            .stream()
                            .filter(column -> (idAsHashKey && column.getIsKey())
                                    || uniqueKeys.contains(column.getName()))
                            .forEach(o -> {
                                if (o.getIsKey()) {
                                    hash.computeIfAbsent(key, s -> new ArrayList<>()).add(o.getValue());
                                } else if (uniqueKeys.contains(o.getName())) {
                                    hash.computeIfAbsent(keyOf(entryWrapper, o.getName()),
                                            s -> new ArrayList<>()).add(o.getValue());
                                }
                            });
                    if (!CollectionUtils.isEmpty(combineKeys)) {
                        Map<String, Object> columnMap = SqlTypeConverter.convertToActualType(consumerConfig,
                                rowData.getBeforeColumnsList(), schemaName, tableName, false);
                        for (List<String> columnList : combineKeys) {
                            String keySuffix = RedisKey.suffix(columnList);
                            String name = RedisKey.hashKey(columnList, columnMap);
                            hash.computeIfAbsent(keyOf(entryWrapper, keySuffix),
                                    s -> new ArrayList<>()).add(name);
                        }
                    }
                });
        return hash;
    }

    @Override
    public boolean filterable() {
        return false;
    }

    @Override
    public void accept(Map<String, List<String>> hash) {
        HashOperations<String, Object, Object> ops = redisTemplate.opsForHash();
        redisTemplate.execute((RedisConnection rc) -> {
            hash.forEach((s, strings) -> {
                Object[] objects = strings.toArray();
                ops.delete(s, objects);
            });
            return null;
        });
    }
}
