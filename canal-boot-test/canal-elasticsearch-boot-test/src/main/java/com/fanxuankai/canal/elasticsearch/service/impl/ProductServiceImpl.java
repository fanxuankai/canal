package com.fanxuankai.canal.elasticsearch.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fanxuankai.canal.elasticsearch.domain.Product;
import com.fanxuankai.canal.elasticsearch.mapper.ProductMapper;
import com.fanxuankai.canal.elasticsearch.service.ProductService;
import org.springframework.stereotype.Service;

/**
 * @author fanxuankai
 */
@Service
public class ProductServiceImpl extends ServiceImpl<ProductMapper, Product> implements ProductService {

}
