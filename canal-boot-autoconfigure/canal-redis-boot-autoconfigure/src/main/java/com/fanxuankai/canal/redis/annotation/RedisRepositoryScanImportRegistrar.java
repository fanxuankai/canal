package com.fanxuankai.canal.redis.annotation;

import com.fanxuankai.canal.redis.RedisRepositoryRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.lang.NonNull;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class RedisRepositoryScanImportRegistrar implements ImportBeanDefinitionRegistrar {
    @Override
    public void registerBeanDefinitions(@NonNull AnnotationMetadata importingClassMetadata,
                                        @NonNull BeanDefinitionRegistry registry) {
        AnnotationAttributes attributes = AnnotationAttributes
                .fromMap(importingClassMetadata.getAnnotationAttributes(RedisRepositoryScan.class.getName()));
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
                RedisRepositoryRegistry.registerWith(registry, basePackages);
            }
        }
    }
}
