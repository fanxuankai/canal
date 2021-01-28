package com.fanxuankai.canal.clickhouse.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.consumer.AbstractDbConsumer;
import com.fanxuankai.canal.db.core.util.SqlUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * @author fanxuankai
 */
public class InsertConsumer extends AbstractDbConsumer {

    public InsertConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public void accept(List<String> batchSql) {
        jdbcTemplate.batchUpdate(batchSql.toArray(new String[0]));
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        return SqlUtils.convertInsert(entryWrapper, canalDbConfiguration);
    }

}
