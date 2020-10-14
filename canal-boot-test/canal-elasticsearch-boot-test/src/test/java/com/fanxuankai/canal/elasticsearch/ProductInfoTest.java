package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.elasticsearch.domain.Attribute;
import com.fanxuankai.canal.elasticsearch.domain.Product;
import com.fanxuankai.canal.elasticsearch.domain.ProductAttribute;
import com.fanxuankai.canal.elasticsearch.service.AttributeService;
import com.fanxuankai.canal.elasticsearch.service.ProductAttributeService;
import com.fanxuankai.canal.elasticsearch.service.ProductService;
import com.fanxuankai.commons.util.concurrent.Threads;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author fanxuankai
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class ProductInfoTest {
    @Resource
    private ProductService productService;
    @Resource
    private AttributeService attributeService;
    @Resource
    private ProductAttributeService productAttributeService;

    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Test
    public void add() {
        List<Attribute> attributeList = new ArrayList<>();
        Attribute attribute = new Attribute();
        attribute.setId(1L);
        attribute.setType(1);
        attribute.setName("red");
        attribute.setValue("红色");
        attributeList.add(attribute);
        attribute = new Attribute();
        attribute.setId(2L);
        attribute.setType(1);
        attribute.setName("black");
        attribute.setValue("黑色");
        attributeList.add(attribute);
        attribute = new Attribute();
        attribute.setId(3L);
        attribute.setType(1);
        attribute.setName("white");
        attribute.setValue("白色");
        attributeList.add(attribute);
        attributeService.saveBatch(attributeList);

        Product product = new Product();
        product.setId(1L);
        product.setCode("6P");
        product.setName("iPhone6 Plus");
        productService.save(product);

        List<ProductAttribute> productAttributeList = new ArrayList<>();
        ProductAttribute productAttribute = new ProductAttribute();
        productAttribute.setId(1L);
        productAttribute.setProductId(1L);
        productAttribute.setAttributeId(1L);
        productAttributeList.add(productAttribute);
        productAttribute = new ProductAttribute();
        productAttribute.setId(2L);
        productAttribute.setProductId(1L);
        productAttribute.setAttributeId(2L);
        productAttributeList.add(productAttribute);
        productAttribute = new ProductAttribute();
        productAttribute.setId(3L);
        productAttribute.setProductId(1L);
        productAttribute.setAttributeId(3L);
        productAttributeList.add(productAttribute);
        productAttributeService.saveBatch(productAttributeList);

        Threads.sleep(5, TimeUnit.SECONDS);
    }

    @Test
    public void updateByQuery() throws IOException {
//        RestHighLevelClient client = elasticsearchRestTemplate.getClient();
//        UpdateByQueryRequest request = new UpdateByQueryRequest("canal_client_example.product_info");
//        request.setConflicts("proceed");
//        request.setDocTypes("doc");
//        QueryBuilder query = QueryBuilders.termsQuery("code", "6P");
//        request.setQuery(query);
//        Map<String, Object> params = new HashMap<>();
//        params.put("name", "范旋凯");
//        request.setScript(new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", "ctx._source.name = 'fxk';", params));
//        request.setRefresh(true);
//        long updated = client.updateByQuery(request, RequestOptions.DEFAULT).getUpdated();
//        System.out.println(updated);

        RestHighLevelClient client = elasticsearchRestTemplate.getClient();
        //参数为索引名，可以不指定，可以一个，可以多个
        UpdateByQueryRequest request = new UpdateByQueryRequest("canal_client_example.product_info");
        // 更新时版本冲突
        request.setConflicts("proceed");
        // 设置查询条件，第一个参数是字段名，第二个参数是字段的值
        request.setQuery(new TermQueryBuilder("code.keyword", "6P"));
        // 更新最大文档数
        request.setSize(10);
        // 批次大小
        request.setBatchSize(1000);
//		request.setPipeline("my_pipeline");

        request.setScript(new Script(ScriptType.INLINE, "painless",
                "ctx._source.name = 'fxk2';", Collections.emptyMap()));
        // 并行
        request.setSlices(2);
        // 使用滚动参数来控制“搜索上下文”存活的时间
        request.setScroll(TimeValue.timeValueMinutes(10));
        // 如果提供路由，则将路由复制到滚动查询，将流程限制为匹配该路由值的切分
//		request.setRouting("=cat");

        // 可选参数
        // 超时
        request.setTimeout(TimeValue.timeValueMinutes(2));
        // 刷新索引
        request.setRefresh(true);

        System.out.println(client.updateByQuery(request, RequestOptions.DEFAULT).getUpdated());

    }
}
