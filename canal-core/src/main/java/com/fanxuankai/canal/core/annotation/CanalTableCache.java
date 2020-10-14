package com.fanxuankai.canal.core.annotation;

import com.google.common.base.CaseFormat;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalTableCache {
    private static final Map<Class<?>, String> SCHEMA_CACHE = new HashMap<>();
    private static final Map<Class<?>, String> TABLE_CACHE = new HashMap<>();

    public static String getTableName(Class<?> domainType) {
        String tableName = TABLE_CACHE.get(domainType);
        if (tableName != null) {
            return tableName;
        }
        CanalTable canalTable = AnnotationUtils.findAnnotation(domainType, CanalTable.class);
        Optional<TableAttributes> optionalTableAttributes = TableAttributes.from(domainType);
        tableName = Optional.ofNullable(canalTable).map(CanalTable::name).orElse("");
        if (StringUtils.isEmpty(tableName)) {
            tableName = optionalTableAttributes.map(TableAttributes::getName).orElse("");
            if (StringUtils.isEmpty(tableName)) {
                tableName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, domainType.getSimpleName());
            }
        }
        TABLE_CACHE.put(domainType, tableName);
        return tableName;
    }

    public static String getSchema(Class<?> domainType) {
        String schema = SCHEMA_CACHE.get(domainType);
        if (schema != null) {
            return schema;
        }
        CanalTable canalTable = AnnotationUtils.findAnnotation(domainType, CanalTable.class);
        schema = Optional.ofNullable(canalTable).map(CanalTable::schema).orElse("");
        Optional<TableAttributes> optionalTableAttributes = TableAttributes.from(domainType);
        if (StringUtils.isEmpty(schema)) {
            schema = optionalTableAttributes.map(TableAttributes::getSchema).orElse("");
            if (StringUtils.isEmpty(schema)) {
                schema = DefaultSchemaAttributes.getSchema();
                if (StringUtils.isEmpty(schema)) {
                    throw new RuntimeException(String.format("无法找到 %s 所对应的数据库名", domainType.getName()));
                }
            }
        }
        SCHEMA_CACHE.put(domainType, schema);
        return schema;
    }

}
