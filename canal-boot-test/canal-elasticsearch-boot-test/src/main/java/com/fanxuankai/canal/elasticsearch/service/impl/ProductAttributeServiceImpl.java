package com.fanxuankai.canal.elasticsearch.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fanxuankai.canal.elasticsearch.domain.ProductAttribute;
import com.fanxuankai.canal.elasticsearch.mapper.ProductAttributeMapper;
import com.fanxuankai.canal.elasticsearch.service.ProductAttributeService;
import org.springframework.stereotype.Service;

/**
 * @author fanxuankai
 */
@Service
public class ProductAttributeServiceImpl extends ServiceImpl<ProductAttributeMapper,
        ProductAttribute> implements ProductAttributeService {

}
