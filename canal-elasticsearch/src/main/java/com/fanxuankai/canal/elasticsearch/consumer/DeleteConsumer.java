package com.fanxuankai.canal.elasticsearch.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.CommonUtils;
import com.fanxuankai.canal.core.util.DomainConverter;
import com.fanxuankai.canal.elasticsearch.*;
import com.fanxuankai.canal.elasticsearch.config.CanalElasticsearchConfiguration;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 删除事件消费者
 *
 * @author fanxuankai
 */
@Slf4j
public class DeleteConsumer extends AbstractEsConsumer<List<Object>> {

    public DeleteConsumer(CanalElasticsearchConfiguration canalElasticsearchConfiguration,
                          IndexDefinitionManager indexDefinitionManager,
                          ElasticsearchRestTemplate elasticsearchRestTemplate) {
        super(canalElasticsearchConfiguration, indexDefinitionManager, elasticsearchRestTemplate);
    }

    @Override
    public List<Object> apply(EntryWrapper entryWrapper) {
        return indexDefinitionManager.getIndexDefinitions(entryWrapper.getSchemaName(), entryWrapper.getTableName())
                .stream()
                .map(indexDefinition -> queries(entryWrapper, indexDefinition))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public boolean filterable() {
        return false;
    }

    @Override
    public void accept(List<Object> objects) {
        if (CollectionUtils.isEmpty(objects)) {
            return;
        }
        Optional.of(objects.stream().filter(o -> o instanceof UpdateQuery)
                .map(o -> (UpdateQuery) o)
                .collect(Collectors.toList()))
                .filter(o -> !o.isEmpty())
                .ifPresent(updateQueries -> {
                    try {
                        elasticsearchRestTemplate.bulkUpdate(updateQueries);
                    } catch (Exception e) {
                        log.debug(e.getLocalizedMessage());
                    }
                });
        objects.stream().filter(o -> o instanceof DeleteObject)
                .map(o -> (DeleteObject) o)
                .forEach(deleteObject -> elasticsearchRestTemplate.delete(deleteObject.getDocClass(),
                        deleteObject.getId()));
        objects.stream().filter(o -> o instanceof UpdateByQueryParam)
                .map(o -> (UpdateByQueryParam) o)
                .forEach(updateByQueryParam -> UpdateConsumer.updateByQuery(elasticsearchRestTemplate,
                        updateByQueryParam));
    }

    private List<Object> queries(EntryWrapper entryWrapper, IndexDefinition indexDefinition) {
        DocumentFunction<Object, Object> function = indexDefinition.getDocumentFunction();
        return entryWrapper.getAllRowDataList()
                .stream()
                .map(CanalEntry.RowData::getBeforeColumnsList)
                .map(columns -> DomainConverter.of(CommonUtils.toJsonString(columns),
                        indexDefinition.getEntityClass()))
                .map(t -> {
                    if (function instanceof MasterDocumentFunction) {
                        String id = ((MasterDocumentFunction<Object, Object>) function).applyForDelete(t);
                        return Collections.singletonList(new DeleteObject().setId(id).setDocClass(indexDefinition.getDocumentClass()));
                    } else if (function instanceof OneToOneDocumentFunction) {
                        return Collections.singletonList(((OneToOneDocumentFunction<Object, Object>) function).applyForDelete(t));
                    } else if (function instanceof ManyToOneDocumentFunction) {
                        return ((ManyToOneDocumentFunction<Object, Object>) function).applyForDelete(t);
                    } else if (function instanceof OneToManyDocumentFunction) {
                        UpdateByQuery updateByQuery =
                                ((OneToManyDocumentFunction<Object, Object>) function).applyForDelete(t);
                        return Collections.singletonList(new UpdateByQueryParam()
                                .setUpdateByQuery(updateByQuery)
                                .setIndexDefinition(indexDefinition));
                    }
                    return Collections.emptyList();
                })
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(object -> {
                    if (function instanceof MasterDocumentFunction || object instanceof UpdateByQueryParam) {
                        return object;
                    }
                    UpdateQuery query = new UpdateQuery();
                    query.setClazz(object.getClass());
                    query.setId(getId(object));
                    query.setUpdateRequest(new UpdateRequest()
                            .doc(JSON.toJSONString(object), XContentType.JSON));
                    return query;
                })
                .collect(Collectors.toList());
    }

    @Data
    @Accessors(chain = true)
    private static final class DeleteObject {
        private Class<?> docClass;
        private String id;
    }
}
