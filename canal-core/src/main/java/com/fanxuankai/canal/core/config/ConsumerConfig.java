package com.fanxuankai.canal.core.config;

import com.fanxuankai.canal.core.model.Filter;
import lombok.Data;

import java.util.Map;

/**
 * @author fanxuankai
 */
@Data
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

}