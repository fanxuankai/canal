package com.fanxuankai.canal.clickhouse.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.util.ConvertUpdateFunction;
import com.fanxuankai.canal.db.core.util.SqlUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractClickhouseConsumer {

    public UpdateConsumer(JdbcTemplate jdbcTemplate, CanalDbConfiguration canalDbConfiguration) {
        super(jdbcTemplate, canalDbConfiguration);
    }

    @Override
    public List<String> apply(EntryWrapper entryWrapper) {
        return SqlUtils.convertUpdate(entryWrapper, canalDbConfiguration, new ConvertUpdateFunction() {
            @Override
            public String apply(String schemaName, String tableName, String setSql, String idName, String idValue) {
                return String.format("ALTER TABLE %s.%s UPDATE %s where %s = '%s'",
                        schemaName, tableName, setSql, idName, idValue);
            }

            @Override
            public String apply(String schemaName, String tableName, String setSql, String idName,
                                List<String> idValues) {
                return String.format("ALTER TABLE %s.%s UPDATE %s where %s in (%s)",
                        schemaName, tableName, setSql, idName, idValues.stream()
                                .map(s -> "'" + s + "'")
                                .collect(Collectors.joining(",")));
            }
        });
    }

}
