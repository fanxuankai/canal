package com.fanxuankai.canal.db.core.consumer;

import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.util.SqlUtils;
import com.fanxuankai.commons.util.ThrowableUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Objects;

/**
 * @author fanxuankai
 */
public abstract class AbstractDbConsumer implements EntryConsumer<List<String>> {
    protected final JdbcTemplate jdbcTemplate;
    protected final CanalDbConfiguration canalDbConfiguration;

    public AbstractDbConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        this.jdbcTemplate = jdbcTemplate;
        this.canalDbConfiguration = canalDbConfiguration;
    }

    @Override
    public void accept(List<String> batchSql) {
        try {
            SqlUtils.executeBatch(Objects.requireNonNull(jdbcTemplate.getDataSource()), batchSql);
        } catch (Throwable throwable) {
            ThrowableUtils.checkException(throwable, DuplicateKeyException.class,
                    SQLIntegrityConstraintViolationException.class);
        }
    }

}
