package com.fanxuankai.canal.mysql.consumer;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.consumer.AbstractDbConsumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class InsertConsumer extends AbstractDbConsumer {

    public InsertConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("insert into %s", canalDbConfiguration.getTableName(entryWrapper)));
        List<ColumnMapInfo> columnMapInfos = columnMap(entryWrapper, true);
        List<String> newNames = columnMapInfos.stream().map(ColumnMapInfo::getNewName).collect(Collectors.toList());
        sb.append(String.format(" ( %s ) ", String.join(",", newNames)));
        sb.append("values\n");
        sb.append(entryWrapper.getAllRowDataList()
                .stream()
                .map(o -> o.getAfterColumnsList().stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                        CanalEntry.Column::getValue)))
                .map(map -> columnMapInfos.stream()
                        .map(columnMapInfo -> {
                            if (columnMapInfo.getName() == null) {
                                return "'" + columnMapInfo.getDefaultValue() + "'";
                            }
                            String value = map.get(columnMapInfo.getName());
                            if (!StringUtils.hasText(value)) {
                                value = columnMapInfo.getDefaultValue();
                            }
                            if (StringUtils.hasText(value)) {
                                return "'" + value + "'";
                            }
                            return null;
                        })
                        .collect(Collectors.joining(",", "(", ")")))
                .collect(Collectors.joining(",")));
        return Collections.singletonList(sb.toString());
    }

}
