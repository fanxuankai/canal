package com.fanxuankai.canal.elasticsearch.domain;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fanxuankai.canal.elasticsearch.annotation.Index;
import com.fanxuankai.canal.elasticsearch.annotation.Indexes;
import com.fanxuankai.canal.elasticsearch.document.ProductInfo;
import com.fanxuankai.canal.elasticsearch.function.ProductAttributeFunction;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
@TableName(value = "t_product_attribute", schema = "canal_client_example")
@Indexes({
        @Index(documentClass = ProductInfo.class, documentFunctionClass = ProductAttributeFunction.class)
})
public class ProductAttribute {
    private Long id;
    private Long productId;
    private Long attributeId;
}
