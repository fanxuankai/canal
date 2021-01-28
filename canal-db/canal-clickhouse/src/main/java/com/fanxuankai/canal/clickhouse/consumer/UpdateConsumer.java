package com.fanxuankai.canal.clickhouse.consumer;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.consumer.AbstractDbConsumer;
import com.fanxuankai.canal.db.core.model.ColumnMapInfo;
import com.fanxuankai.canal.db.core.util.SqlUtils;
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
    public void accept(List<String> batchSql) {
        jdbcTemplate.batchUpdate(batchSql.toArray(new String[0]));
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        List<ColumnMapInfo> columnMapInfos = SqlUtils.columnMap(entryWrapper, canalDbConfiguration, false);
        String tableName = canalDbConfiguration.getTableName(entryWrapper);
        return entryWrapper.getAllRowDataList().stream()
                .map(o -> o.getAfterColumnsList().stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                        CanalEntry.Column::getValue)))
                .map(map -> {
                    String setSql = columnMapInfos.stream().map(columnMapInfo -> {
                        String value = map.get(columnMapInfo.getName());
                        if (!StringUtils.hasText(value)) {
                            return columnMapInfo.getNewName() + " = null";
                        }
                        return String.format("%s = '%s'", columnMapInfo.getNewName(), value);
                    }).collect(Collectors.joining(","));
                    String idName = null;
                    String idValue = null;
                    for (ColumnMapInfo columnMapInfo : columnMapInfos) {
                        if (columnMapInfo.isPrimary()) {
                            idName = columnMapInfo.getNewName();
                            idValue = map.get(columnMapInfo.getName());
                        }
                    }
                    return String.format("ALTER TABLE %s UPDATE %s where %s = '%s'", tableName, setSql, idName,
                            idValue);
                })
                .collect(Collectors.toList());
    }

}
