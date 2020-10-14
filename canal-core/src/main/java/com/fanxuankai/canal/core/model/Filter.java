package com.fanxuankai.canal.core.model;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * 数据过滤
 *
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class Filter {
    /**
     * Aviator 表达式, 表达式必须返回 true or false
     */
    private String aviatorExpression;
    /**
     * 字段值发生变化
     */
    private List<String> updatedColumns;
    /**
     * 任一字段的值发生变化
     */
    private boolean anyUpdated;
    /**
     * 事件类型过滤, 默认为增、删、改、删表
     */
    private List<CanalEntry.EventType> eventTypes = Arrays.asList(INSERT, UPDATE, DELETE, ERASE);
}
