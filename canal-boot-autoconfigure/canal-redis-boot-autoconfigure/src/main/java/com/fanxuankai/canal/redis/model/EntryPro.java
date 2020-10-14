package com.fanxuankai.canal.redis.model;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class EntryPro {
    private final String name;
    private final List<Object> values;
}
