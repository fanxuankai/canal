package com.fanxuankai.canal.elasticsearch;

import lombok.Data;
import lombok.experimental.Accessors;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class UpdateByQuery {
    private QueryBuilder queryBuilder;
    private Map<String, Object> data;
}