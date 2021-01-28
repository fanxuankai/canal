package com.fanxuankai.canal.mysql.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.consumer.AbstractDbConsumer;
import com.fanxuankai.canal.db.core.util.SqlUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractDbConsumer {

    public UpdateConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        return SqlUtils.convertUpdate(entryWrapper, canalDbConfiguration);
    }
}
