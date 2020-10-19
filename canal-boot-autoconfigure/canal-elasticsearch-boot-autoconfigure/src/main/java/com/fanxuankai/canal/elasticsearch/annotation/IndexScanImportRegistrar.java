package com.fanxuankai.canal.elasticsearch.annotation;

import com.fanxuankai.canal.elasticsearch.AnnotationIndexDefinition;
import com.fanxuankai.canal.elasticsearch.IndexDefinition;
import com.fanxuankai.canal.elasticsearch.IndexDefinitionManager;
import com.fanxuankai.canal.elasticsearch.IndexScanner;
import com.fanxuankai.canal.elasticsearch.config.CanalEsAutoConfiguration;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.lang.NonNull;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class IndexScanImportRegistrar implements ImportBeanDefinitionRegistrar {
    @Override
    public void registerBeanDefinitions(@NonNull AnnotationMetadata importingClassMetadata,
                                        @NonNull BeanDefinitionRegistry registry) {
        AnnotationAttributes attributes = AnnotationAttributes
                .fromMap(importingClassMetadata.getAnnotationAttributes(IndexScan.class.getName()));
        if (attributes != null) {
            List<String> basePackages = new ArrayList<>();
            basePackages.addAll(Arrays.stream(attributes.getStringArray("value"))
                    .filter(StringUtils::hasText)
                    .collect(Collectors.toSet()));
            basePackages.addAll(Arrays.stream(attributes.getStringArray("basePackages"))
                    .filter(StringUtils::hasText)
                    .collect(Collectors.toSet()));
            basePackages.addAll(Arrays.stream(attributes.getClassArray("basePackageClasses"))
                    .map(ClassUtils::getPackageName)
                    .collect(Collectors.toList()));
            if (!basePackages.isEmpty()) {
                IndexDefinitionManager indexDefinitionManager = loadFrom(IndexScanner.scan(basePackages));
                registry.registerBeanDefinition(CanalEsAutoConfiguration.class.getName(),
                        BeanDefinitionBuilder
                                .genericBeanDefinition(CanalEsAutoConfiguration.class)
                                .addConstructorArgValue(indexDefinitionManager)
                                .getBeanDefinition());
            }
        }
    }

    private IndexDefinitionManager loadFrom(Set<Class<?>> classSet) {
        IndexDefinitionManager indexDefinitionManager = new IndexDefinitionManager();
        if (CollectionUtils.isEmpty(classSet)) {
            return indexDefinitionManager;
        }
        for (Class<?> domainClass : classSet) {
            Indexes indexes = AnnotationUtils.findAnnotation(domainClass, Indexes.class);
            if (indexes == null) {
                continue;
            }
            for (Index index : indexes.value()) {
                IndexDefinition definition = new AnnotationIndexDefinition(domainClass, index);
                indexDefinitionManager.add(definition);
            }
        }
        return indexDefinitionManager;
    }

}
