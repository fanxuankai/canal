package com.fanxuankai.canal.clickhouse.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.util.SqlUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.List;

/**
 * @author fanxuankai
 */
public class DeleteConsumer extends AbstractClickhouseConsumer {

    public DeleteConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        return Collections.singletonList(SqlUtils.convertDelete(entryWrapper, canalDbConfiguration,
                (schemaName, tableName, idName, ids)
                        -> String.format("ALTER TABLE %s.%s DELETE where %s in ( %s )",
                        schemaName, tableName, idName, ids)));
    }

}
