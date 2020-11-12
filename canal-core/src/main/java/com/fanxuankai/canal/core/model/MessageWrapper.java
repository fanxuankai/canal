package com.fanxuankai.canal.core.model;

import com.alibaba.otter.canal.protocol.Message;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class MessageWrapper {
    private final Message raw;
    private final int rowDataCountBeforeFilter;
    private List<EntryWrapper> entryWrapperList;

    public MessageWrapper(Message raw) {
        this.raw = raw;
        this.entryWrapperList = raw.getEntries().stream().map(EntryWrapper::new).collect(Collectors.toList());
        this.rowDataCountBeforeFilter = getRowDataCountAfterFilter();
    }

    public int getRowDataCountBeforeFilter() {
        return rowDataCountBeforeFilter;
    }

    public List<EntryWrapper> getEntryWrapperList() {
        return entryWrapperList;
    }

    public void setEntryWrapperList(List<EntryWrapper> entryWrapperList) {
        this.entryWrapperList = entryWrapperList;
    }

    public long getBatchId() {
        return raw.getId();
    }

    public int getRowDataCountAfterFilter() {
        return this.entryWrapperList
                .stream()
                .map(EntryWrapper::getRawRowDataCount)
                .reduce(Integer::sum)
                .orElse(0);
    }
}
