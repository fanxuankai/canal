package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.elasticsearch.annotation.Index;
import com.fanxuankai.canal.elasticsearch.annotation.Indexes;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * @author fanxuankai
 */
public class IndexDefinitionManager {

    private final Map<String, Map<String, List<IndexDefinition>>> bySchemaAndTable
            = new HashMap<>(16);

    public static IndexDefinitionManager from(Set<Class<?>> domainClasses) {
        IndexDefinitionManager indexDefinitionManager = new IndexDefinitionManager();
        if (CollectionUtils.isEmpty(domainClasses)) {
            return indexDefinitionManager;
        }
        for (Class<?> domainClass : domainClasses) {
            Indexes indexes = AnnotationUtils.findAnnotation(domainClass, Indexes.class);
            if (indexes == null) {
                continue;
            }
            for (Index index : indexes.value()) {
                IndexDefinition definition = new SimpleIndexDefinition(domainClass, index);
                indexDefinitionManager.add(definition);
            }
        }
        return indexDefinitionManager;
    }

    public static IndexDefinitionManager fromBasePackages(List<String> basePackages) {
        return from(IndexScanner.scan(basePackages));
    }

    public void add(IndexDefinition indexDefinition) {
        bySchemaAndTable.computeIfAbsent(indexDefinition.getSchema(), s -> new HashMap<>(16))
                .computeIfAbsent(indexDefinition.getTableName(), s -> new ArrayList<>())
                .add(indexDefinition);
    }

    public List<IndexDefinition> getIndexDefinitions(String schema, String tableName) {
        return Optional.ofNullable(bySchemaAndTable.get(schema))
                .map(o -> o.get(tableName))
                .orElse(Collections.emptyList());
    }

}
