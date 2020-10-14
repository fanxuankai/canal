package com.fanxuankai.canal.elasticsearch.domain;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
@TableName(value = "t_attribute", schema = "canal_client_example")
public class Attribute {
    private Long id;
    private Integer type;
    private String name;
    private String value;
}
