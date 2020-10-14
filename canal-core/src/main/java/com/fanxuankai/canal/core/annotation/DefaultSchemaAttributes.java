package com.fanxuankai.canal.core.annotation;

import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

import java.util.Map;
import java.util.Optional;

/**
 * DefaultSchema 注解属性工具类
 *
 * @author fanxuankai
 */
public class DefaultSchemaAttributes {

    private static AnnotationAttributes attributes;

    public static void from(AnnotationMetadata metadata) {
        Map<String, Object> annotationAttributes = metadata.getAnnotationAttributes(DefaultSchema.class.getName(),
                false);
        attributes = AnnotationAttributes.fromMap(annotationAttributes);
        if (attributes == null) {
            throw new IllegalArgumentException(String.format(
                    "@%s is not present on importing class '%s' as expected",
                    DefaultSchema.class.getSimpleName(), metadata.getClassName()));
        }
    }

    public static String getSchema() {
        return Optional.ofNullable(attributes)
                .map(annotationAttributes -> annotationAttributes.getString("value"))
                .orElse(null);
    }

}
