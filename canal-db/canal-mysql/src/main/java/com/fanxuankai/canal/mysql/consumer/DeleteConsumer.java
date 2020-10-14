package com.fanxuankai.canal.mysql.consumer;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.consumer.AbstractDbConsumer;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class DeleteConsumer extends AbstractDbConsumer {

    public DeleteConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        String idName = entryWrapper.getAllRowDataList().get(0)
                .getBeforeColumnsList()
                .stream()
                .filter(CanalEntry.Column::getIsKey)
                .map(CanalEntry.Column::getName)
                .findFirst()
                .orElse(null);
        String ids = entryWrapper.getAllRowDataList().stream()
                .map(CanalEntry.RowData::getBeforeColumnsList)
                .map(columns -> columns.stream().filter(CanalEntry.Column::getIsKey)
                        .findFirst()
                        .map(CanalEntry.Column::getValue)
                        .map(s -> "'" + s + "'")
                        .orElse(null))
                .filter(Objects::nonNull)
                .collect(Collectors.joining(","));
        idName = Optional.ofNullable(canalDbConfiguration.getColumnMap(entryWrapper).get(idName)).orElse(idName);
        return Collections.singletonList(String.format("delete from %s where %s in ( %s )",
                entryWrapper.getTableName(), idName, ids));
    }
}
