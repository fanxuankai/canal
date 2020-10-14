package com.fanxuankai.canal.redis.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Redis Repository 接口扫描
 *
 * @author fanxuankai
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import(RedisRepositoryScanImportRegistrar.class)
public @interface RedisRepositoryScan {

    /**
     * Alias for the {@link #basePackages()} attribute. Allows for more concise annotation declarations e.g.:
     * {@code @RedisRepositoryScan("org.my.pkg")} instead of {@code @RedisRepositoryScan(basePackages = "org.my.pkg"})}.
     *
     * @return base package names
     */
    String[] value() default {};

    /**
     * Base packages to scan for RedisRepository interfaces. Note that only interfaces with at least one method will be
     * registered; concrete classes will be ignored.
     *
     * @return base package names for scanning RedisRepository interface
     */
    String[] basePackages() default {};

    /**
     * Type-safe alternative to {@link #basePackages()} for specifying the packages to scan for annotated components.
     * The
     * package of each class specified will be scanned.
     * <p>
     * Consider creating a special no-op marker class or interface in each package that serves no purpose other than
     * being
     * referenced by this attribute.
     *
     * @return classes that indicate base package for scanning RedisRepository interface
     */
    Class<?>[] basePackageClasses() default {};
}
