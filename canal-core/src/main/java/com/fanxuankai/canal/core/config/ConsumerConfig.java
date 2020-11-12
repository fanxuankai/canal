package com.fanxuankai.canal.core.config;

import com.fanxuankai.canal.core.model.Filter;

import java.util.Map;

/**
 * @author fanxuankai
 */
public class ConsumerConfig {
    /**
     * 过滤
     */
    private Filter filter;
    /**
     * 逻辑删除字段
     */
    private String logicDeleteField;
    /**
     * Java 类型, key: columnName, value: className
     */
    private Map<String, String> javaTypeMap;

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public String getLogicDeleteField() {
        return logicDeleteField;
    }

    public void setLogicDeleteField(String logicDeleteField) {
        this.logicDeleteField = logicDeleteField;
    }

    public Map<String, String> getJavaTypeMap() {
        return javaTypeMap;
    }

    public void setJavaTypeMap(Map<String, String> javaTypeMap) {
        this.javaTypeMap = javaTypeMap;
    }
}