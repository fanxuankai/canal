package com.fanxuankai.canal.db.core.util;

/**
 * 列信息
 *
 * @author fanxuankai
 */
class ColumnInfo {
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
}