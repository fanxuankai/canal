package com.fanxuankai.canal.db.core.util;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.google.common.collect.Lists;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
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
    public static String convertInsert(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("insert into %s.%s", canalDbConfiguration.getSchemaName(entryWrapper),
                canalDbConfiguration.getTableName(entryWrapper)));
        List<ColumnInfo> columnInfos = columnMap(entryWrapper, canalDbConfiguration, true);
        List<String> newNames = columnInfos.stream().map(ColumnInfo::getNewName).collect(Collectors.toList());
        sb.append(String.format(" ( %s ) ", String.join(",", newNames)));
        sb.append("values\n");
        sb.append(entryWrapper.getAllRowDataList()
                .stream()
                .map(o -> o.getAfterColumnsList().stream().collect(Collectors.toMap(CanalEntry.Column::getName,
                        CanalEntry.Column::getValue)))
                .map(map -> columnInfos.stream()
                        .map(columnInfo -> {
                            if (columnInfo.getName() == null) {
                                return "'" + columnInfo.getDefaultValue() + "'";
                            }
                            String value = map.get(columnInfo.getName());
                            if (!StringUtils.hasText(value)) {
                                value = columnInfo.getDefaultValue();
                            }
                            if (StringUtils.hasText(value)) {
                                return "'" + value + "'";
                            }
                            return null;
                        })
                        .collect(Collectors.joining(",", "(", ")")))
                .collect(Collectors.joining(",")));
        return sb.toString();
    }

    /**
     * 转 update 语句
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @return List
     */
    public static List<String> convertUpdate(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration) {
        return convertUpdate(entryWrapper, canalDbConfiguration, new ConvertUpdateFunction() {
            @Override
            public String apply(String schemaName, String tableName, String setSql, String idName, String idValue) {
                return String.format("update %s.%s set %s where %s = '%s'",
                        schemaName, tableName, setSql, idName, idValue);
            }

            @Override
            public String apply(String schemaName, String tableName, String setSql, String idName,
                                List<String> idValues) {
                return String.format("update %s.%s set %s where %s in (%s)",
                        schemaName, tableName, setSql, idName, idValues.stream()
                                .map(s -> "'" + s + "'")
                                .collect(Collectors.joining(",")));
            }
        });
    }

    /**
     * 转 update 语句
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @return List
     */
    public static List<String> convertUpdate(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration,
                                             ConvertUpdateFunction function) {
        // 忽略变化的字段
        List<String> ignoreChangeColumns = canalDbConfiguration.getIgnoreChangeColumns(entryWrapper);
        List<String> sqlList = Lists.newArrayList();
        // Entry 转 ColumnInfo
        Map<String, ColumnInfo> columnInfoMap = columnMap(entryWrapper, canalDbConfiguration, false)
                .stream()
                .collect(Collectors.toMap(ColumnInfo::getName, o -> o));
        List<List<CanalEntry.Column>> combineColumnList = Lists.newArrayList();
        List<List<CanalEntry.Column>> combinePkColumnList = Lists.newArrayList();
        Function<List<CanalEntry.Column>, CanalEntry.Column> pkColumnFunction = columns -> columns.stream()
                .filter(CanalEntry.Column::getIsKey)
                .findFirst()
                .orElse(null);
        // 第一轮或者下一轮合并开始
        boolean begin = true;
        for (CanalEntry.RowData rowData : entryWrapper.getAllRowDataList()) {
            List<CanalEntry.Column> currentColumns = Lists.newArrayList();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                if (column.getUpdated() && !ignoreChangeColumns.contains(column.getName())) {
                    currentColumns.add(column);
                }
            }
            if (currentColumns.isEmpty()) {
                continue;
            }
            if (begin) {
                combineColumnList.add(currentColumns);
                combinePkColumnList.add(Lists.newArrayList(pkColumnFunction.apply(rowData.getAfterColumnsList())));
                begin = false;
                continue;
            }
            List<CanalEntry.Column> lastColumnList = combineColumnList.get(combineColumnList.size() - 1);
            List<CanalEntry.Column> lastIdValueList = combinePkColumnList.get(combinePkColumnList.size() - 1);
            Map<String, CanalEntry.Column> lastColumnMap =
                    lastColumnList.stream().collect(Collectors.toMap(CanalEntry.Column::getName, o -> o));
            Map<String, CanalEntry.Column> currentColumnMap =
                    currentColumns.stream().collect(Collectors.toMap(CanalEntry.Column::getName, o -> o));
            if (lastColumnMap.keySet().equals(currentColumnMap.keySet())) {
                if (lastColumnMap.entrySet()
                        .stream()
                        .allMatch(entry -> Objects.equals(currentColumnMap.get(entry.getKey()).getValue(),
                                entry.getValue().getValue()))) {
                    lastIdValueList.add(pkColumnFunction.apply(rowData.getAfterColumnsList()));
                    continue;
                }
            }
            // 设置开始下一轮
            combineColumnList.add(currentColumns);
            combinePkColumnList.add(Lists.newArrayList(pkColumnFunction.apply(rowData.getAfterColumnsList())));
            begin = true;
        }
        String schemaName = canalDbConfiguration.getSchemaName(entryWrapper);
        String tableName = canalDbConfiguration.getTableName(entryWrapper);
        for (int i = 0; i < combineColumnList.size(); i++) {
            List<CanalEntry.Column> columns = combineColumnList.get(i);
            String setSql = columns.stream()
                    .map(column -> {
                        ColumnInfo columnInfo = columnInfoMap.get(column.getName());
                        String value = column.getValue();
                        if (!StringUtils.hasText(value)) {
                            return columnInfo.getNewName() + " = null";
                        }
                        return String.format("%s = '%s'", columnInfo.getNewName(), value);
                    })
                    .collect(Collectors.joining(","));
            List<CanalEntry.Column> pkColumnList = combinePkColumnList.get(i);
            CanalEntry.Column column = pkColumnList.get(0);
            ColumnInfo columnInfo = columnInfoMap.get(column.getName());
            if (pkColumnList.size() > 1) {
                sqlList.add(function.apply(schemaName, tableName, setSql, columnInfo.getNewName(),
                        pkColumnList.stream().map(CanalEntry.Column::getValue).collect(Collectors.toList())));
            } else {
                sqlList.add(function.apply(schemaName, tableName, setSql, columnInfo.getNewName(), column.getValue()));
            }
        }
        return sqlList;
    }

    /**
     * 转 delete 语句
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @return List
     */
    public static String convertDelete(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration) {
        return convertDelete(entryWrapper, canalDbConfiguration, (schemaName, tableName, idName, ids)
                -> String.format("delete from %s.%s where %s in ( %s )",
                schemaName, tableName, idName, ids));
    }

    /**
     * 转 delete 语句
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @return List
     */
    public static String convertDelete(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration,
                                       ConvertDeleteFunction function) {
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
        return function.apply(canalDbConfiguration.getSchemaName(entryWrapper),
                canalDbConfiguration.getTableName(entryWrapper), idName, ids);
    }

    /**
     * 构建列的映射关系
     *
     * @param entryWrapper         数据
     * @param canalDbConfiguration 配置
     * @param insert               /
     * @return list
     */
    public static List<ColumnInfo> columnMap(EntryWrapper entryWrapper, CanalDbConfiguration canalDbConfiguration,
                                             boolean insert) {
        Map<String, String> columnMap = canalDbConfiguration.getColumnMap(entryWrapper);
        List<String> excludeColumns = canalDbConfiguration.getExcludeColumns(entryWrapper);
        List<String> includeColumns = canalDbConfiguration.getIncludeColumns(entryWrapper);
        Map<String, String> defaultValues = canalDbConfiguration.getDefaultValues(entryWrapper);
        List<ColumnInfo> list = entryWrapper.getAllRowDataList().get(0).getAfterColumnsList()
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
                    ColumnInfo columnInfo = new ColumnInfo();
                    columnInfo.setPrimary(column.getIsKey());
                    columnInfo.setName(column.getName());
                    columnInfo.setNewName(newName);
                    columnInfo.setDefaultValue(defaultValues.get(newName));
                    return columnInfo;
                })
                .collect(Collectors.toList());
        if (insert) {
            List<String> names = list.stream().map(ColumnInfo::getName).collect(Collectors.toList());
            for (Map.Entry<String, String> entry : defaultValues.entrySet()) {
                String newName = entry.getKey();
                if (names.contains(newName)) {
                    continue;
                }
                // 目标字段赋默认值
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setNewName(newName);
                columnInfo.setDefaultValue(entry.getValue());
                list.add(columnInfo);
            }
        }
        return list;
    }
}
