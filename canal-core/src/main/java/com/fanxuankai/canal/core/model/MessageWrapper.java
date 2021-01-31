package com.fanxuankai.canal.core.model;

import com.alibaba.otter.canal.protocol.Message;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.google.common.collect.Lists;

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
        if (Objects.equals(canalConfiguration.getMergeEntry().getMerge(), Boolean.TRUE)
                && entryWrapperList.size() > 1) {
            List<EntryWrapper> combineEntryWrapperList = Lists.newArrayList();
            EntryWrapper lastEntryWrapper = entryWrapperList.get(0);
            String lastEventKey = eventKey(lastEntryWrapper);
            for (int i = 1; i < entryWrapperList.size(); i++) {
                EntryWrapper currentEntryWrapper = entryWrapperList.get(i);
                String currentEventKey = eventKey(currentEntryWrapper);
                if (Objects.equals(currentEventKey, lastEventKey)
                        && lastEntryWrapper.getRowDataCount() + currentEntryWrapper.getRowDataCount()
                        <= canalConfiguration.getMergeEntry().getMaxRowDataSize()) {
                    // 满足条件合并 entry
                    lastEntryWrapper.getAllRowDataList().addAll(currentEntryWrapper.getAllRowDataList());
                } else {
                    // 不满足条件开始下一个
                    combineEntryWrapperList.add(lastEntryWrapper);
                    lastEntryWrapper = currentEntryWrapper;
                    lastEventKey = currentEventKey;
                }
            }
            combineEntryWrapperList.add(lastEntryWrapper);
            this.entryWrapperList = combineEntryWrapperList;
        }
        this.rowDataCountBeforeFilter = getRowDataCountAfterFilter();
    }

    private static String eventKey(EntryWrapper entryWrapper) {
        return entryWrapper.getSchemaName()
                + entryWrapper.getTableName()
                + entryWrapper.getEventType();
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
                .map(EntryWrapper::getRowDataCount)
                .reduce(Integer::sum)
                .orElse(0);
    }
}
