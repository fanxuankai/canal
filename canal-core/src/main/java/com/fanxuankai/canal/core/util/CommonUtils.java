package com.fanxuankai.canal.core.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.config.ConsumerConfig;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 公共工具类
 *
 * @author fanxuankai
 */
public class CommonUtils {

    /**
     * 转 MD5
     *
     * @param columns 数据行
     * @return md5
     */
    public static String md5(List<CanalEntry.Column> columns) {
        JSONObject jsonObject = new JSONObject(columns.size(), true);
        columns.forEach(column -> jsonObject.put(column.getName(), column.getValue()));
        String jsonString = jsonObject.toString();
        return DigestUtils.md5Hex(jsonString);
    }

    /**
     * 转 JSON 字符串
     *
     * @param consumerConfig 消费配置
     * @param columnList     数据行
     * @param schema         数据库
     * @param table          表
     * @param localToDate    LocalDate 或者 LocalDateTime 视为 Date 处理
     * @return { columnName: actualValue }
     */
    public static String jsonWithActualType(ConsumerConfig consumerConfig,
                                            List<CanalEntry.Column> columnList,
                                            String schema,
                                            String table,
                                            boolean localToDate) {
        return JSON.toJSONString(SqlTypeConverter.convertToActualType(consumerConfig, columnList, schema, table,
                localToDate),
                SerializerFeature.WriteDateUseDateFormat);
    }

    /**
     * 转 JSON 字符串
     *
     * @param consumerConfig 消费配置
     * @param beforeColumns  旧的数据行
     * @param afterColumns   新的数据行
     * @param schema         数据库
     * @param table          表
     * @param localToDate    LocalDate 或者 LocalDateTime 视为 Date 处理
     * @return [ { columnName: actualValue }, { columnName: actualValue }]
     */
    public static String jsonWithActualType(ConsumerConfig consumerConfig,
                                            List<CanalEntry.Column> beforeColumns,
                                            List<CanalEntry.Column> afterColumns,
                                            String schema,
                                            String table,
                                            boolean localToDate) {
        Map<String, Object> map0 = SqlTypeConverter.convertToActualType(consumerConfig, beforeColumns, schema, table,
                localToDate);
        Map<String, Object> map1 = SqlTypeConverter.convertToActualType(consumerConfig, afterColumns, schema, table,
                localToDate);
        List<Object> list = new ArrayList<>(2);
        list.add(map0);
        list.add(map1);
        return new JSONArray(list).toString(SerializerFeature.WriteDateUseDateFormat);
    }

    /**
     * 所有数据列转为 Map
     *
     * @param columnList 每一列
     * @return HashMap
     */
    public static Map<String, CanalEntry.Column> toColumnMap(List<CanalEntry.Column> columnList) {
        return columnList.stream()
                .collect(Collectors.toMap(CanalEntry.Column::getName, o -> o));
    }

    /**
     * 转 json 字符串
     *
     * @param columnList 数据行
     * @return { columnName: columnValue }
     */
    public static String toJsonString(List<CanalEntry.Column> columnList) {
        return JSON.toJSONString(columnList.stream()
                .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue)));
    }

}
