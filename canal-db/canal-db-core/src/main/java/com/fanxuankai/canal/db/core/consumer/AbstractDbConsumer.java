package com.fanxuankai.canal.db.core.consumer;

import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.commons.util.ThrowableUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
    @Transactional(rollbackFor = Exception.class)
    public void accept(List<String> batchSql) {
        batchSql.stream()
                .filter(StringUtils::hasText)
                .forEach(s -> {
                    try {
                        jdbcTemplate.execute(s);
                    } catch (Throwable throwable) {
                        ThrowableUtils.checkException(throwable, DuplicateKeyException.class,
                                SQLIntegrityConstraintViolationException.class);
                    }
                });
    }

    /**
     * 构建列的映射关系
     *
     * @param entryWrapper 数据
     * @return list
     */
    protected List<ColumnMapInfo> columnMap(EntryWrapper entryWrapper, boolean insert) {
        Map<String, String> columnMap = canalDbConfiguration.getColumnMap(entryWrapper);
        List<String> excludeColumns = canalDbConfiguration.getExcludeColumns(entryWrapper);
        List<String> includeColumns = canalDbConfiguration.getIncludeColumns(entryWrapper);
        Map<String, String> defaultValues = canalDbConfiguration.getDefaultValues(entryWrapper);
        List<ColumnMapInfo> list = entryWrapper.getAllRowDataList().get(0).getAfterColumnsList()
                .stream()
                .filter(column -> {
                    if (!column.getIsKey() && !CollectionUtils.isEmpty(excludeColumns)) {
                        return !excludeColumns.contains(column.getName());
                    }
                    return true;
                })
                .filter(column -> {
                    if (!column.getIsKey() && !CollectionUtils.isEmpty(includeColumns)) {
                        return includeColumns.contains(column.getName());
                    }
                    return true;
                })
                .map(column -> {
                    String newName = Optional.ofNullable(columnMap.get(column.getName())).orElse(column.getName());
                    ColumnMapInfo columnMapInfo = new ColumnMapInfo();
                    columnMapInfo.setPrimary(column.getIsKey());
                    columnMapInfo.setName(column.getName());
                    columnMapInfo.setNewName(newName);
                    columnMapInfo.setDefaultValue(defaultValues.get(newName));
                    return columnMapInfo;
                })
                .collect(Collectors.toList());
        if (insert) {
            List<String> names = list.stream().map(ColumnMapInfo::getName).collect(Collectors.toList());
            for (Map.Entry<String, String> entry : defaultValues.entrySet()) {
                String newName = entry.getKey();
                if (names.contains(newName)) {
                    continue;
                }
                ColumnMapInfo columnMapInfo = new ColumnMapInfo();
                columnMapInfo.setNewName(newName);
                columnMapInfo.setDefaultValue(entry.getValue());
                list.add(columnMapInfo);
            }
        }
        return list;
    }

    protected static class ColumnMapInfo {
        /**
         * 主键
         */
        private boolean primary;
        /**
         * 源列名
         * 为空时意味着 newName 设置了默认值
         */
        private String name;
        /**
         * 目标列名
         */
        private String newName;
        /**
         * 默认值
         */
        private String defaultValue;

        public boolean isPrimary() {
            return primary;
        }

        public void setPrimary(boolean primary) {
            this.primary = primary;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getNewName() {
            return newName;
        }

        public void setNewName(String newName) {
            this.newName = newName;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }
    }
}
