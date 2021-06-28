package com.fanxuankai.canal.core.util;

import cn.hutool.core.text.StrPool;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.mysql.cj.MysqlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.ConversionService;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author fanxuankai
 */
public class SqlTypeConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTypeConverter.class);
    /**
     * key: schema.table value: {字段名: Java 类型}
     */
    private static final Map<String, Map<String, Class<?>>> JAVA_TYPE_CACHE = new ConcurrentHashMap<>();

    /**
     * 转换工具类
     */
    private static final ConversionService CONVERSION_SERVICE = Conversions.getInstance();

    /**
     * 获取 Java 类型
     *
     * @param consumerConfig 消费配置
     * @param columns        列
     * @param schema         数据库
     * @param table          表名
     * @return key: 字段名 value: Java 类型
     */
    public static Map<String, Class<?>> getJavaType(ConsumerConfig consumerConfig,
                                                    List<CanalEntry.Column> columns,
                                                    String schema,
                                                    String table) {
        String key = schema + StrPool.DOT + table;
        Map<String, Class<?>> fieldsTypeMap = JAVA_TYPE_CACHE.get(key);
        if (fieldsTypeMap == null
                // 字段发生了变化, 刷新缓存
                || fieldsTypeMap.size() != columns.size()) {
            fieldsTypeMap = new HashMap<>(columns.size());
            for (CanalEntry.Column column : columns) {
                try {
                    if (consumerConfig != null) {
                        if (!CollectionUtils.isEmpty(consumerConfig.getJavaTypeMap())) {
                            String javaType = consumerConfig.getJavaTypeMap().get(column.getName());
                            if (StringUtils.hasText(javaType)) {
                                fieldsTypeMap.put(column.getName(), Class.forName(javaType));
                                continue;
                            }
                        }
                    }
                    MysqlType mysqlType = MysqlType.getByJdbcType(column.getSqlType());
                    String className = mysqlType.getClassName();
                    if (!Objects.equals(className, "[B")) {
                        fieldsTypeMap.put(column.getName(), Class.forName(className));
                    }
                } catch (Exception e) {
                    LOGGER.error("类型转换失败", e);
                }
            }
            JAVA_TYPE_CACHE.put(key, fieldsTypeMap);
        }
        return fieldsTypeMap;
    }

    /**
     * 数据库表格列转实际类型
     *
     * @param consumerConfig 消费配置
     * @param columns        列
     * @param schema         数据库
     * @param table          表
     * @param localToDate    LocalDate 或者 LocalDateTime 视为 Date 处理
     * @return key: 字段名 value: 字段值
     */
    public static Map<String, Object> convertToActualType(ConsumerConfig consumerConfig,
                                                          List<CanalEntry.Column> columns,
                                                          String schema,
                                                          String table,
                                                          boolean localToDate) {
        Map<String, Class<?>> allFieldsType = SqlTypeConverter.getJavaType(consumerConfig, columns, schema, table);
        Map<String, Object> map = new HashMap<>(columns.size());
        for (CanalEntry.Column column : columns) {
            Object convert = null;
            Class<?> fieldType = allFieldsType.get(column.getName());
            if (fieldType != null && StringUtils.hasText(column.getValue())) {
                if (localToDate) {
                    // Aviator 不支持 LocalDate 和 LocalDateTime 类型
                    // 直接把字符串当做 Date 来处理
                    fieldType = isLocalDateType(fieldType) ? Date.class : fieldType;
                }
                try {
                    convert = CONVERSION_SERVICE.convert(column.getValue(), fieldType);
                } catch (ConversionFailedException e) {
                    LOGGER.error(String.format("%s.%s.%s %s -> %s", schema, table, column.getName(), column.getValue(),
                            fieldType), e);
                }
            }
            map.put(column.getName(), convert);
        }
        return map;
    }

    private static boolean isLocalDateType(Class<?> fieldType) {
        return LocalDate.class.isAssignableFrom(fieldType) || LocalDateTime.class.isAssignableFrom(fieldType);
    }
}
