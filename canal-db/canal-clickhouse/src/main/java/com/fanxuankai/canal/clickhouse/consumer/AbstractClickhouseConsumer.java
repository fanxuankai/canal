package com.fanxuankai.canal.clickhouse.consumer;

import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.commons.util.ThrowableUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;

/**
 * @author fanxuankai
 */
public abstract class AbstractClickhouseConsumer implements EntryConsumer<List<String>> {
    protected final JdbcTemplate jdbcTemplate;
    protected final CanalDbConfiguration canalDbConfiguration;

    public AbstractClickhouseConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        this.jdbcTemplate = jdbcTemplate;
        this.canalDbConfiguration = canalDbConfiguration;
    }

    @Override
    public void accept(List<String> sqlList) {
        try {
            sqlList.forEach(jdbcTemplate::execute);
        } catch (Throwable throwable) {
            ThrowableUtils.checkException(throwable, DuplicateKeyException.class,
                    SQLIntegrityConstraintViolationException.class);
        }
    }
}
