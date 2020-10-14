package com.fanxuankai.canal.mysql.consumer;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.consumer.AbstractDbConsumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractDbConsumer {

    public UpdateConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        String tableName = canalDbConfiguration.getTableName(entryWrapper);
        List<ColumnMapInfo> columnMapInfos = columnMap(entryWrapper, false);
        return entryWrapper.getAllRowDataList().stream()
                .map(o -> o.getAfterColumnsList().stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                        CanalEntry.Column::getValue)))
                .map(map -> {
                    String setSql = columnMapInfos.stream()
                            .map(columnMapInfo -> {
                                String value = map.get(columnMapInfo.getName());
                                if (!StringUtils.hasText(value)) {
                                    return columnMapInfo.getNewName() + " = null";
                                }
                                return String.format("%s = '%s'", columnMapInfo.getNewName(), value);
                            })
                            .collect(Collectors.joining(","));
                    String idName = null;
                    String idValue = null;
                    for (ColumnMapInfo columnMapInfo : columnMapInfos) {
                        if (columnMapInfo.isPrimary()) {
                            idName = columnMapInfo.getNewName();
                            idValue = map.get(columnMapInfo.getName());
                        }
                    }
                    return String.format("update %s set %s where %s = '%s'", tableName, setSql, idName, idValue);
                })
                .collect(Collectors.toList());
    }
}
