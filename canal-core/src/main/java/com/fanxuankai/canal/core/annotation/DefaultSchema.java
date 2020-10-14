package com.fanxuankai.canal.core.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author fanxuankai
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({DefaultSchemaImportRegistrar.class})
public @interface DefaultSchema {

    /**
     * 获取数据库名称的步骤, 先后顺序如下:<p>
     * CanalTable.schema()<p>
     * javax.persistence.Table.schema()<p>
     * DefaultSchema.schema()<p>
     * 若始终无法获取到数据库名称, 程序终止, 抛出异常
     *
     * @return 默认的数据库名称
     */
    String value() default "";

}
