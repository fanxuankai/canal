package com.fanxuankai.canal.db.core.consumer;

import cn.hutool.core.exceptions.ExceptionUtil;
import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.util.SqlUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

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
    @SuppressWarnings("unchecked")
    public void accept(List<String> batchSql) {
        try {
            if (CollectionUtils.isEmpty(batchSql)) {
                return;
            }
            if (batchSql.size() == 1) {
                jdbcTemplate.execute(batchSql.get(0));
                return;
            }
            SqlUtils.executeBatch(Objects.requireNonNull(jdbcTemplate.getDataSource()), batchSql);
        } catch (Throwable throwable) {
            if (!ExceptionUtil.isCausedBy(throwable, DuplicateKeyException.class,
                    SQLIntegrityConstraintViolationException.class)) {
                throw new RuntimeException(throwable);
            }
        }
    }

}
