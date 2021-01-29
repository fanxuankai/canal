package com.fanxuankai.canal.clickhouse.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.util.SqlUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractClickhouseConsumer {

    public UpdateConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        return SqlUtils.convertUpdate(entryWrapper, canalDbConfiguration,
                (schemaName, tableName, setSql, idName, idValue)
                        -> String.format("ALTER TABLE %s.%s UPDATE %s where %s = '%s'",
                        schemaName, tableName, setSql, idName, idValue));
    }

}
