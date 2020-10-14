package com.fanxuankai.canal.elasticsearch.function;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.fanxuankai.boot.commons.util.BeanUtils;
import com.fanxuankai.canal.elasticsearch.MasterDocumentFunction;
import com.fanxuankai.canal.elasticsearch.document.ProductInfo;
import com.fanxuankai.canal.elasticsearch.domain.Product;
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
public class ProductFunction implements MasterDocumentFunction<Product, ProductInfo> {
    @Resource
    private AttributeService attributeService;
    @Resource
    private ProductAttributeService productAttributeService;

    @Override
    public ProductInfo applyForInsert(Product insert) {
        return new ProductInfo()
                .setId(insert.getId())
                .setCode(insert.getCode())
                .setName(insert.getName())
                .setAttributeList(Optional.of(
                        productAttributeService.list(Wrappers.lambdaQuery(ProductAttribute.class)
                                .eq(ProductAttribute::getProductId, insert.getId()))
                                .stream()
                                .map(ProductAttribute::getAttributeId)
                                .collect(Collectors.toList())
                )
                        .filter(o -> !o.isEmpty())
                        .map(ids -> attributeService.listByIds(ids))
                        .orElse(Collections.emptyList()));
    }

    @Override
    public ProductInfo applyForUpdate(Product before, Product after) {
        return BeanUtils.copyProperties(after, ProductInfo.class);
    }

    @Override
    public String applyForDelete(Product delete) {
        return delete.getId().toString();
    }
}
