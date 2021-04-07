package com.fanxuankai.canal.core.annotation;

import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author fanxuankai
 */
public class TableAttributes {

    private static final Map<Class<?>, TableAttributes> CACHE = new ConcurrentHashMap<>();
    private static final String TABLE_ANNOTATION_NAME = "javax.persistence.Table";
    private static final String MYBATIS_PLUS_TABLE_ANNOTATION_NAME = "com.baomidou.mybatisplus.annotation.TableName";
    private final AnnotationAttributes annotationAttributes;

    public TableAttributes(AnnotationAttributes annotationAttributes) {
        this.annotationAttributes = annotationAttributes;
    }

    public static Optional<TableAttributes> from(Class<?> type) {
        TableAttributes tableAttributes = CACHE.get(type);
        if (tableAttributes == null) {
            AnnotationAttributes annotationAttributes = fromType(type);
            if (annotationAttributes == null) {
                return Optional.empty();
            }
            tableAttributes = new TableAttributes(annotationAttributes);
            CACHE.put(type, tableAttributes);
        }
        return Optional.of(tableAttributes);
    }

    private static AnnotationAttributes fromType(Class<?> type) {
        AnnotationAttributes annotationAttributes = fromAnnotation(type, TABLE_ANNOTATION_NAME);
        if (annotationAttributes == null) {
            annotationAttributes = fromAnnotation(type, MYBATIS_PLUS_TABLE_ANNOTATION_NAME);
        }
        return annotationAttributes;
    }

    @SuppressWarnings("unchecked")
    private static AnnotationAttributes fromAnnotation(Class<?> type, String annotation) {
        try {
            Class<Annotation> tableNameClass = (Class<Annotation>) Class.forName(annotation);
            Annotation tableNameAnnotation = AnnotationUtils.findAnnotation(type, tableNameClass);
            if (tableNameAnnotation != null) {
                return AnnotationAttributes.fromMap(AnnotationUtils.getAnnotationAttributes(tableNameAnnotation));
            }
        } catch (Exception ignored) {

        }
        return null;
    }

    public String getName() {
        String annotationTypeName = Optional.ofNullable(annotationAttributes.annotationType())
                .map(Class::getName)
                .orElse(null);
        if (Objects.equals(annotationTypeName, TABLE_ANNOTATION_NAME)) {
            return annotationAttributes.getString("name");
        } else if (Objects.equals(annotationTypeName, MYBATIS_PLUS_TABLE_ANNOTATION_NAME)) {
            return annotationAttributes.getString("value");
        }
        return null;
    }

    public String getSchema() {
        return annotationAttributes.getString("schema");
    }

}
