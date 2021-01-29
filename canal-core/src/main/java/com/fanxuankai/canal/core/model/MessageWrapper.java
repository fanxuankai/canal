package com.fanxuankai.canal.core.model;

import com.alibaba.otter.canal.protocol.Message;
import com.fanxuankai.canal.core.config.CanalConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class MessageWrapper {
    private final Message raw;
    private final int rowDataCountBeforeFilter;
    private List<EntryWrapper> entryWrapperList;

    public MessageWrapper(Message raw, CanalConfiguration canalConfiguration) {
        this.raw = raw;
        this.entryWrapperList = raw.getEntries()
                .stream()
                .map(EntryWrapper::new)
                .collect(Collectors.toList());
        // 合并
        if (Objects.equals(canalConfiguration.getMergeEntry(), Boolean.TRUE)
                && entryWrapperList.size() > 1) {
            if (entryWrapperList.stream()
                    .map(entryWrapper -> entryWrapper.getSchemaName()
                            + entryWrapper.getTableName()
                            + entryWrapper.getEventType())
                    .count() == 1) {
                // 相同 schema、table、eventType, 合并为一个 Entry
                EntryWrapper last = entryWrapperList.get(entryWrapperList.size() - 1);
                last.setAllRowDataList(entryWrapperList.stream()
                        .flatMap(o -> o.getAllRowDataList().stream())
                        .collect(Collectors.toList()));
                this.entryWrapperList = Collections.singletonList(last);
            }
        }
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
