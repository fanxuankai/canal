package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.elasticsearch.annotation.IndexScan;
import com.fanxuankai.canal.elasticsearch.domain.ProductAttribute;
import com.fanxuankai.canal.elasticsearch.mapper.AttributeMapper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author fanxuankai
 */
@SpringBootApplication
@IndexScan(basePackageClasses = {ProductAttribute.class})
@MapperScan(basePackageClasses = {AttributeMapper.class})
public class CanalElasticsearchApplication {
    public static void main(String[] args) {
        SpringApplication.run(CanalElasticsearchApplication.class, args);
    }
}
