package com.fanxuankai.canal.elasticsearch;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class UpdateByQueryParam {
    private IndexDefinition indexDefinition;
    private UpdateByQuery updateByQuery;
}