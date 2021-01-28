package com.fanxuankai.canal.db.core.util;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.db.core.model.ColumnMapInfo;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public final class SqlUtils {
    public static void executeBatch(DataSource dataSource, List<String> sqlList) throws Exception {
        Connection conn = dataSource.getConnection();
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            try (Statement st = conn.createStatement()) {
                for (String sql : sqlList) {
                    st.addBatch(sql);
                }
                st.executeBatch();
            }
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
            conn.close();
        }
    }

    /**
     * 转 insert 语句
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @return List
     */
    public static List<String> convertInsert(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("insert into %s", canalDbConfiguration.getTableName(entryWrapper)));
        List<ColumnMapInfo> columnMapInfos = columnMap(entryWrapper, canalDbConfiguration, true);
        List<String> newNames = columnMapInfos.stream().map(ColumnMapInfo::getNewName).collect(Collectors.toList());
        sb.append(String.format(" ( %s ) ", String.join(",", newNames)));
        sb.append("values\n");
        sb.append(entryWrapper.getAllRowDataList()
                .stream()
                .map(o -> o.getAfterColumnsList().stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                        CanalEntry.Column::getValue)))
                .map(map -> columnMapInfos.stream()
                        .map(columnMapInfo -> {
                            if (columnMapInfo.getName() == null) {
                                return "'" + columnMapInfo.getDefaultValue() + "'";
                            }
                            String value = map.get(columnMapInfo.getName());
                            if (!StringUtils.hasText(value)) {
                                value = columnMapInfo.getDefaultValue();
                            }
                            if (StringUtils.hasText(value)) {
                                return "'" + value + "'";
                            }
                            return null;
                        })
                        .collect(Collectors.joining(",", "(", ")")))
                .collect(Collectors.joining(",")));
        return Collections.singletonList(sb.toString());
    }

    /**
     * 转 update 语句
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @return List
     */
    public static List<String> convertUpdate(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration) {
        String tableName = canalDbConfiguration.getTableName(entryWrapper);
        List<ColumnMapInfo> columnMapInfos = columnMap(entryWrapper, canalDbConfiguration, false);
        return entryWrapper.getAllRowDataList().stream()
                .map(o -> o.getAfterColumnsList().stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                        CanalEntry.Column::getValue)))
                .map(map -> {
                    String setSql = columnMapInfos.stream()
                            .map(columnMapInfo -> {
                                String value = map.get(columnMapInfo.getName());
                                if (!StringUtils.hasText(value)) {
                                    return columnMapInfo.getNewName() + " = null";
                                }
                                return String.format("%s = '%s'", columnMapInfo.getNewName(), value);
                            })
                            .collect(Collectors.joining(","));
                    String idName = null;
                    String idValue = null;
                    for (ColumnMapInfo columnMapInfo : columnMapInfos) {
                        if (columnMapInfo.isPrimary()) {
                            idName = columnMapInfo.getNewName();
                            idValue = map.get(columnMapInfo.getName());
                        }
                    }
                    return String.format("update %s set %s where %s = '%s'", tableName, setSql, idName, idValue);
                })
                .collect(Collectors.toList());
    }

    /**
     * 转 delete 语句
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @return List
     */
    public static List<String> convertDelete(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration) {
        String idName = entryWrapper.getAllRowDataList().get(0)
                .getBeforeColumnsList()
                .stream()
                .filter(CanalEntry.Column::getIsKey)
                .map(CanalEntry.Column::getName)
                .findFirst()
                .orElse(null);
        String ids = entryWrapper.getAllRowDataList().stream()
                .map(CanalEntry.RowData::getBeforeColumnsList)
                .map(columns -> columns.stream().filter(CanalEntry.Column::getIsKey)
                        .findFirst()
                        .map(CanalEntry.Column::getValue)
                        .map(s -> "'" + s + "'")
                        .orElse(null))
                .filter(Objects::nonNull)
                .collect(Collectors.joining(","));
        idName = Optional.ofNullable(canalDbConfiguration.getColumnMap(entryWrapper).get(idName)).orElse(idName);
        return Collections.singletonList(String.format("delete from %s where %s in ( %s )",
                entryWrapper.getTableName(), idName, ids));
    }

    /**
     * 构建列的映射关系
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @param insert               /
     * @return list
     */
    public static List<ColumnMapInfo> columnMap(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration,
                                                boolean insert) {
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
}
