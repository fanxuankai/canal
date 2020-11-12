package com.fanxuankai.canal.db.core.config;

import com.fanxuankai.canal.core.config.ConsumerConfig;

import java.util.List;
import java.util.Map;

/**
 * @author fanxuankai
 */
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getIncludeColumns() {
        return includeColumns;
    }

    public void setIncludeColumns(List<String> includeColumns) {
        this.includeColumns = includeColumns;
    }

    public List<String> getExcludeColumns() {
        return excludeColumns;
    }

    public void setExcludeColumns(List<String> excludeColumns) {
        this.excludeColumns = excludeColumns;
    }

    public Map<String, String> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(Map<String, String> columnMap) {
        this.columnMap = columnMap;
    }

    public Map<String, String> getDefaultValues() {
        return defaultValues;
    }

    public void setDefaultValues(Map<String, String> defaultValues) {
        this.defaultValues = defaultValues;
    }
}