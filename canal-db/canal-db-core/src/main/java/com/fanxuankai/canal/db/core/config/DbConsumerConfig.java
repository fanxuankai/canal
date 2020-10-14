package com.fanxuankai.canal.db.core.config;

import com.fanxuankai.canal.core.config.ConsumerConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

/**
 * @author fanxuankai
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class DbConsumerConfig extends ConsumerConfig {
    /**
     * 表名
     */
    private String tableName;
    /**
     * 包含的列
     */
    private List<String> includeColumns;
    /**
     * 排除的列
     */
    private List<String> excludeColumns;
    /**
     * key: 源字段名 value: 目标字段名
     */
    private Map<String, String> columnMap;
    /**
     * 目标数据库设默认值
     */
    private Map<String, String> defaultValues;
}