package com.fanxuankai.canal.core.model;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * 数据过滤
 *
 * @author fanxuankai
 */
public class Filter {
    /**
     * Aviator 表达式, 表达式必须返回 true or false
     */
    private String aviatorExpression;
    /**
     * 字段值发生变化
     */
    private List<String> updatedColumns;
    /**
     * 任一字段的值发生变化
     */
    private boolean anyUpdated;
    /**
     * 事件类型过滤, 默认为增、删、改、删表
     */
    private List<CanalEntry.EventType> eventTypes = Arrays.asList(INSERT, UPDATE, DELETE, ERASE);

    public String getAviatorExpression() {
        return aviatorExpression;
    }

    public void setAviatorExpression(String aviatorExpression) {
        this.aviatorExpression = aviatorExpression;
    }

    public List<String> getUpdatedColumns() {
        return updatedColumns;
    }

    public void setUpdatedColumns(List<String> updatedColumns) {
        this.updatedColumns = updatedColumns;
    }

    public boolean isAnyUpdated() {
        return anyUpdated;
    }

    public void setAnyUpdated(boolean anyUpdated) {
        this.anyUpdated = anyUpdated;
    }

    public List<CanalEntry.EventType> getEventTypes() {
        return eventTypes;
    }

    public void setEventTypes(List<CanalEntry.EventType> eventTypes) {
        this.eventTypes = eventTypes;
    }
}
