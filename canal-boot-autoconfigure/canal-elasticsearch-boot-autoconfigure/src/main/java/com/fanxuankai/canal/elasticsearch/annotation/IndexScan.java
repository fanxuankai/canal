package com.fanxuankai.canal.elasticsearch.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author fanxuankai
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import({IndexScanImportRegistrar.class})
public @interface IndexScan {

    String[] value() default {};

    String[] basePackages() default {};

    Class<?>[] basePackageClasses() default {};
}
