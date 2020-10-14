package com.fanxuankai.canal.elasticsearch.function;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.fanxuankai.canal.elasticsearch.OneToOneDocumentFunction;
import com.fanxuankai.canal.elasticsearch.document.ProductInfo;
import com.fanxuankai.canal.elasticsearch.domain.ProductAttribute;
import com.fanxuankai.canal.elasticsearch.service.AttributeService;
import com.fanxuankai.canal.elasticsearch.service.ProductAttributeService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
@Service
public class ProductAttributeFunction implements OneToOneDocumentFunction<ProductAttribute, ProductInfo> {
    @Resource
    private AttributeService attributeService;
    @Resource
    private ProductAttributeService productAttributeService;

    @Override
    public ProductInfo applyForUpdate(ProductAttribute before, ProductAttribute after) {
        return applyForDelete(after);
    }

    @Override
    public ProductInfo applyForDelete(ProductAttribute delete) {
        return new ProductInfo()
                .setId(delete.getProductId())
                .setAttributeList(Optional.of(
                        productAttributeService.list(Wrappers.lambdaQuery(ProductAttribute.class)
                                .eq(ProductAttribute::getProductId, delete.getProductId()))
                                .stream()
                                .map(ProductAttribute::getAttributeId)
                                .collect(Collectors.toList())
                )
                        .filter(o -> !o.isEmpty())
                        .map(ids -> attributeService.listByIds(ids))
                        .orElse(Collections.emptyList()));
    }
}
