package com.fanxuankai.canal.clickhouse.consumer;

import cn.hutool.core.exceptions.ExceptionUtil;
import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
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
    @SuppressWarnings("unchecked")
    public void accept(List<String> sqlList) {
        try {
            sqlList.forEach(jdbcTemplate::execute);
        } catch (Throwable throwable) {
            if (!ExceptionUtil.isCausedBy(throwable, DuplicateKeyException.class,
                    SQLIntegrityConstraintViolationException.class)) {
                throw new RuntimeException(throwable);
            }
        }
    }
}
