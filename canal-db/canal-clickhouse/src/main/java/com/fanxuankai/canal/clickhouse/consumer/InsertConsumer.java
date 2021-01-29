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
public class InsertConsumer extends AbstractClickhouseConsumer {

    public InsertConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        return Collections.singletonList(SqlUtils.convertInsert(entryWrapper, canalDbConfiguration));
    }
}
