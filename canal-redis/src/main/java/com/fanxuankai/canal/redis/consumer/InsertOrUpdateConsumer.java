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

import java.util.List;
import java.util.Map;

/**
 * 新增事件消费者
 *
 * @author fanxuankai
 */
public class InsertOrUpdateConsumer extends AbstractRedisConsumer<Map<String, Map<String, Object>>> {

    public InsertOrUpdateConsumer(CanalRedisConfiguration canalRedisConfiguration,
                                  RedisTemplate<String, Object> redisTemplate) {
        super(canalRedisConfiguration, redisTemplate);
    }

    @Override
    public Map<String, Map<String, Object>> apply(EntryWrapper entryWrapper) {
        List<String> uniqueKeys = canalRedisConfiguration.getUniqueKeys(entryWrapper);
        boolean idAsHashKey = canalRedisConfiguration.isIdAsHashKey(entryWrapper);
        List<List<String>> combineKeys = canalRedisConfiguration.getCombineKeys(entryWrapper);
        Map<String, Map<String, Object>> map = Maps.newHashMap();
        String key = keyOf(entryWrapper);
        String schemaName = entryWrapper.getSchemaName();
        String tableName = entryWrapper.getTableName();
        ConsumerConfig consumerConfig = getConsumerConfig(entryWrapper);
        entryWrapper.getAllRowDataList().forEach(rowData -> {
            Map<String, Object> hashValue = SqlTypeConverter.convertToActualType(consumerConfig,
                    rowData.getAfterColumnsList(), schemaName, tableName, false);
            rowData.getAfterColumnsList()
                    .stream()
                    .filter(column -> (idAsHashKey && column.getIsKey())
                            || uniqueKeys.contains(column.getName()))
                    .forEach(column -> {
                        if (column.getIsKey()) {
                            map.computeIfAbsent(key, s -> Maps.newHashMap()).put(column.getValue(), hashValue);
                        } else if (uniqueKeys.contains(column.getName())) {
                            map.computeIfAbsent(keyOf(entryWrapper, column.getName()),
                                    s -> Maps.newHashMap()).put(column.getValue(), hashValue);
                        }
                    });
            if (!CollectionUtils.isEmpty(combineKeys)) {
                for (List<String> columnList : combineKeys) {
                    String keySuffix = RedisKey.suffix(columnList);
                    String hashKey = RedisKey.hashKey(columnList, hashValue);
                    map.computeIfAbsent(keyOf(entryWrapper, keySuffix), s -> Maps.newHashMap())
                            .put(hashKey, hashValue);
                }
            }
        });
        return map;
    }

    @Override
    public void accept(Map<String, Map<String, Object>> hash) {
        if (hash.isEmpty()) {
            return;
        }
        HashOperations<String, Object, Object> opsForHash = redisTemplate.opsForHash();
        redisTemplate.execute((RedisConnection rc) -> {
            hash.forEach(opsForHash::putAll);
            return null;
        });
    }

}
