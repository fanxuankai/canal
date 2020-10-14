package com.fanxuankai.canal.redis.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class Entry {
    private final String name;
    private final Object value;
}
