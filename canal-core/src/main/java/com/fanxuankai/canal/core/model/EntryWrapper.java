package com.fanxuankai.canal.core.model;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * @author fanxuankai
 */
public class EntryWrapper {
    private final CanalEntry.Entry raw;
    /**
     * RowChange 所有数据, 可变集合, 支持数据过滤
     */
    private List<CanalEntry.RowData> allRowDataList;
    private CanalEntry.EventType eventType;

    public EntryWrapper(CanalEntry.Entry raw) {
        this.raw = raw;
        this.eventType = raw.getHeader().getEventType();
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(raw.getStoreValue());
            allRowDataList = new ArrayList<>(rowChange.getRowDatasList());
        } catch (Exception e) {
            throw new RuntimeException("error parse " + raw.toString());
        }
    }

    public CanalEntry.Entry getRaw() {
        return raw;
    }

    public List<CanalEntry.RowData> getAllRowDataList() {
        return allRowDataList;
    }

    public void setAllRowDataList(List<CanalEntry.RowData> allRowDataList) {
        this.allRowDataList = allRowDataList;
    }

    public CanalEntry.EventType getEventType() {
        return eventType;
    }

    public void setEventType(CanalEntry.EventType eventType) {
        this.eventType = eventType;
    }

    public String getSchemaName() {
        return raw.getHeader().getSchemaName();
    }

    public String getTableName() {
        return raw.getHeader().getTableName();
    }

    public String getLogfileName() {
        return raw.getHeader().getLogfileName();
    }

    public long getLogfileOffset() {
        return raw.getHeader().getLogfileOffset();
    }

    public int getRawRowDataCount() {
        return allRowDataList.size();
    }

    @Override
    public String toString() {
        return String.format("%s.%s %s.%s.%s rowDataCount: %s", getLogfileName(), getLogfileOffset(), getSchemaName(),
                getTableName(), getEventType(), getRawRowDataCount());
    }
}
