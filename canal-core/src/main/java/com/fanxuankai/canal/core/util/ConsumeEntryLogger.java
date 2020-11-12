package com.fanxuankai.canal.core.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.model.EntryWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * @author fanxuankai
 */
public class ConsumeEntryLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeEntryLogger.class);

    public static void log(LogInfo logInfo) {
        EntryWrapper entryWrapper = logInfo.entryWrapper;
        LogRowChange logRowChange = new LogRowChange(logInfo.batchId, logInfo.time, entryWrapper);
        CanalConfiguration canalConfiguration = logInfo.getCanalConfiguration();
        if (canalConfiguration.isShowRowChange()) {
            List<List<LogColumn>> list = entryWrapper.getAllRowDataList().stream()
                    .map(o -> logColumns(o, entryWrapper.getEventType()))
                    .collect(Collectors.toList());
            LOGGER.info("{}{}\n{}", "[" + canalConfiguration.getId() + "] ", logRowChange.toString(),
                    JSON.toJSONString(list,
                            canalConfiguration.isFormatRowChangeLog()));
        } else {
            LOGGER.info("{}{}", "[" + canalConfiguration.getId() + "] ", logRowChange.toString());
        }
    }

    private static List<LogColumn> logColumns(CanalEntry.RowData rowData, CanalEntry.EventType eventType) {
        if (eventType == DELETE || eventType == ERASE) {
            return rowData.getBeforeColumnsList().stream()
                    .map(column -> new LogColumn(column.getName(), column.getValue(), null, false))
                    .collect(Collectors.toList());
        } else if (eventType == INSERT) {
            return rowData.getAfterColumnsList().stream()
                    .map(column -> new LogColumn(column.getName(), null, column.getValue(), true))
                    .collect(Collectors.toList());
        } else if (eventType == UPDATE) {
            List<LogColumn> logColumns = new ArrayList<>(rowData.getAfterColumnsCount());
            for (int i = 0; i < rowData.getAfterColumnsList().size(); i++) {
                CanalEntry.Column bColumn = rowData.getBeforeColumnsList().get(i);
                CanalEntry.Column aColumn = rowData.getAfterColumnsList().get(i);
                logColumns.add(new LogColumn(aColumn.getName(), bColumn.getValue(), aColumn.getValue(),
                        aColumn.getUpdated()));
            }
            return logColumns;
        }
        return Collections.emptyList();
    }

    public static class LogInfo {
        private CanalConfiguration canalConfiguration;
        private EntryWrapper entryWrapper;
        private long batchId;
        private long time;

        public CanalConfiguration getCanalConfiguration() {
            return canalConfiguration;
        }

        public void setCanalConfiguration(CanalConfiguration canalConfiguration) {
            this.canalConfiguration = canalConfiguration;
        }

        public EntryWrapper getEntryWrapper() {
            return entryWrapper;
        }

        public void setEntryWrapper(EntryWrapper entryWrapper) {
            this.entryWrapper = entryWrapper;
        }

        public long getBatchId() {
            return batchId;
        }

        public void setBatchId(long batchId) {
            this.batchId = batchId;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }

    private static class LogRowChange {
        private long batchId;
        private long time;
        private EntryWrapper entryWrapper;

        public LogRowChange(long batchId, long time, EntryWrapper entryWrapper) {
            this.batchId = batchId;
            this.time = time;
            this.entryWrapper = entryWrapper;
        }

        public long getBatchId() {
            return batchId;
        }

        public void setBatchId(long batchId) {
            this.batchId = batchId;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public EntryWrapper getEntryWrapper() {
            return entryWrapper;
        }

        public void setEntryWrapper(EntryWrapper entryWrapper) {
            this.entryWrapper = entryWrapper;
        }

        @Override
        public String toString() {
            return String.format("%s batchId: %s time: %sms", entryWrapper.toString(), batchId, time);
        }
    }

    private static class LogColumn {
        private String name;
        private String oldValue;
        private String value;
        private boolean updated;

        public LogColumn(String name, String oldValue, String value, boolean updated) {
            this.name = name;
            this.oldValue = oldValue;
            this.value = value;
            this.updated = updated;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getOldValue() {
            return oldValue;
        }

        public void setOldValue(String oldValue) {
            this.oldValue = oldValue;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public boolean isUpdated() {
            return updated;
        }

        public void setUpdated(boolean updated) {
            this.updated = updated;
        }
    }
}
