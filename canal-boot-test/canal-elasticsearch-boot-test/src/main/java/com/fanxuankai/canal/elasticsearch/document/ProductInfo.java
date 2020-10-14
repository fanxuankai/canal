package com.fanxuankai.canal.elasticsearch.document;

import com.fanxuankai.canal.elasticsearch.domain.Attribute;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.List;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
@Document(indexName = "canal_client_example.product_info", type = "doc")
public class ProductInfo {
    @Id
    private Long id;
    private String code;
    private String name;
    private List<Attribute> attributeList;
}
