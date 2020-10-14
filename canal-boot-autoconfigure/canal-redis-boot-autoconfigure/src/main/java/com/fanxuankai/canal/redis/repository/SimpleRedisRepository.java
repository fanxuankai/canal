package com.fanxuankai.canal.redis.repository;

import com.alibaba.fastjson.JSON;
import com.fanxuankai.canal.core.annotation.CanalTableCache;
import com.fanxuankai.canal.core.util.RedisKey;
import com.fanxuankai.canal.redis.annotation.CanalRedis;
import com.fanxuankai.canal.redis.model.CombineKeyModel;
import com.fanxuankai.canal.redis.model.Entry;
import com.fanxuankai.canal.redis.model.UniqueKey;
import com.fanxuankai.canal.redis.model.UniqueKeyPro;
import com.google.common.collect.Sets;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * RedisRepository 实现类
 *
 * @author fanxuankai
 */
public class SimpleRedisRepository<T, ID> implements RedisRepository<T, ID> {

    private final CanalRedis canalRedis;
    private final Class<T> domainType;
    private final RedisTemplate<Object, Object> redisTemplate;
    private final String schema;
    private final String tableName;

    /**
     * 构造器
     *
     * @param domainType    the domain type
     * @param redisTemplate the RedisTemplate
     */
    public SimpleRedisRepository(Class<T> domainType, RedisTemplate<Object, Object> redisTemplate) {
        this.domainType = domainType;
        this.redisTemplate = redisTemplate;
        this.canalRedis = AnnotationUtils.findAnnotation(domainType, CanalRedis.class);
        this.schema = CanalTableCache.getSchema(domainType);
        this.tableName = CanalTableCache.getTableName(domainType);
    }

    @Override
    public Optional<T> findById(ID id) {
        return Optional.ofNullable(convert(redisTemplate.opsForHash().get(key(), id.toString())));
    }

    @Override
    public boolean existsById(ID id) {
        return findById(id).isPresent();
    }

    @Override
    public List<T> findAll() {
        return getAll(key());
    }

    @Override
    public List<T> findAllById(Iterable<ID> ids) {
        if (ids == null) {
            return Collections.emptyList();
        }
        Set<Object> idSet = Sets.newHashSet();
        for (Object id : ids) {
            idSet.add(id.toString());
        }
        return multiGet(key(), idSet);
    }

    @Override
    public long count() {
        return redisTemplate.opsForHash().size(key());
    }

    @Override
    public T getOne(ID id) {
        Optional<T> optional = findById(id);
        if (!optional.isPresent()) {
            throw new NullPointerException();
        }
        return optional.get();
    }

    @Override
    public Optional<T> findOne(UniqueKey uniqueKey) {
        return Optional.ofNullable(convert(redisTemplate.opsForHash().get(keyWithSuffix(uniqueKey.getName()),
                uniqueKey.getValue().toString())));
    }

    @Override
    public boolean exists(UniqueKey uniqueKey) {
        return findOne(uniqueKey).isPresent();
    }

    @Override
    public Optional<T> findOne(CombineKeyModel combineKeyModel) {
        List<Entry> entries = combineKeyModel.getEntries();
        List<String> names = entries.stream().map(Entry::getName).collect(Collectors.toList());
        String suffix = RedisKey.suffix(names);
        String key = keyWithSuffix(suffix);
        String hashKey = RedisKey.hashKey(names, entries.stream().collect(Collectors.toMap(Entry::getName,
                o -> o.getValue().toString())));
        return Optional.ofNullable(convert(redisTemplate.opsForHash().get(key, hashKey)));
    }

    @Override
    public T getOne(CombineKeyModel combineKeyModel) {
        Optional<T> optional = findOne(combineKeyModel);
        if (!optional.isPresent()) {
            throw new NullPointerException();
        }
        return optional.get();
    }

    @Override
    public List<T> findAll(List<String> combineKey) {
        String suffix = RedisKey.suffix(combineKey);
        String key = keyWithSuffix(suffix);
        return getAll(key);
    }

    @Override
    public List<T> findAll(UniqueKeyPro uniqueKeyPro) {
        String key = keyWithSuffix(uniqueKeyPro.getName());
        Set<Object> hashKeys = new HashSet<>();
        for (Object value : uniqueKeyPro.getValues()) {
            hashKeys.add(value.toString());
        }
        return multiGet(key, hashKeys);
    }

    @Override
    public T getOne(UniqueKey uniqueKey) {
        Optional<T> optional = findOne(uniqueKey);
        if (!optional.isPresent()) {
            throw new NullPointerException();
        }
        return optional.get();
    }

    @Override
    public List<T> findAll(String uniqueKey) {
        return getAll(keyWithSuffix(uniqueKey));
    }

    private List<T> getAll(String key) {
        return convertList(redisTemplate.opsForHash().values(key));
    }

    private List<T> multiGet(String key, Collection<Object> hashKeys) {
        return convertList(redisTemplate.opsForHash().multiGet(key, hashKeys));
    }

    private List<T> convertList(List<Object> objects) {
        objects = objects.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(objects)) {
            return Collections.emptyList();
        }
        return objects.stream()
                .map(o -> JSON.parseObject(JSON.toJSONString(o), domainType))
                .collect(Collectors.toList());
    }

    private T convert(Object o) {
        if (o == null) {
            return null;
        }
        return JSON.parseObject(JSON.toJSONString(o), domainType);
    }

    private String key() {
        if (canalRedis != null) {
            return canalRedis.key();
        }
        return RedisKey.of(schema, tableName);
    }

    private String keyWithSuffix(String suffix) {
        if (canalRedis != null) {
            return RedisKey.withSuffix(canalRedis.key(), suffix);
        }
        return RedisKey.of(schema, tableName, suffix);
    }

}
