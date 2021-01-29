package com.fanxuankai.canal.db.core.model;

/**
 * @author fanxuankai
 */
public class ColumnMapInfo {
    /**
     * 主键
     */
    private boolean primary;

    /**
     * 源列名
     * 为空时意味着 newName 设置了默认值
     */
    private String name;

    /**
     * 目标列名
     */
    private String newName;

    /**
     * 默认值
     */
    private String defaultValue;

    /**
     * 是否更新
     */
    private boolean updated;

    public boolean isPrimary() {
        return primary;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }
}