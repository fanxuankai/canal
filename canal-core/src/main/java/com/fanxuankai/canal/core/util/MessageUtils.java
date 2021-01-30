package com.fanxuankai.canal.core.util;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.model.Filter;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.google.common.collect.Lists;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author fanxuankai
 */
public class MessageUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageUtils.class);

    /**
     * 逻辑删除转换
     *
     * @param wrapper               MessageWrapper
     * @param canalConfiguration    CanalConfiguration
     * @param consumerConfigFactory ConsumerConfigFactory
     */
    public static void logicDeleteConvert(MessageWrapper wrapper, CanalConfiguration canalConfiguration,
                                          ConsumerConfigFactory consumerConfigFactory) {
        if (!canalConfiguration.isEnableLogicDelete()) {
            return;
        }
        List<EntryWrapper> logicDeletedEntryWrapperList = Lists.newArrayList();
        for (EntryWrapper entryWrapper : wrapper.getEntryWrapperList()) {
            if (entryWrapper.getEventType() != CanalEntry.EventType.UPDATE) {
                continue;
            }
            String logicDeleteField = consumerConfigFactory.get(entryWrapper)
                    .map(ConsumerConfig::getLogicDeleteField)
                    .orElse(null);
            if (!StringUtils.hasText(logicDeleteField)) {
                logicDeleteField = canalConfiguration.getLogicDeleteField();
            }
            String fLogicDeleteField = logicDeleteField;
            List<CanalEntry.RowData> logicDeletedRowDataList = entryWrapper.getAllRowDataList()
                    .stream()
                    .filter(rowData -> rowData.getAfterColumnsList().stream()
                            .anyMatch(column -> Objects.equals(fLogicDeleteField, column.getName())
                                    && Objects.equals(Conversions.getInstance().convert(column.getValue(),
                                    Boolean.class), Boolean.TRUE)))
                    .collect(Collectors.toList());
            if (!logicDeletedRowDataList.isEmpty()) {
                EntryWrapper logicDeletedEntryWrapper = new EntryWrapper();
                logicDeletedEntryWrapper.setRaw(entryWrapper.getRaw());
                logicDeletedEntryWrapper.setEventType(CanalEntry.EventType.DELETE);
                logicDeletedRowDataList.forEach(rowData -> {
                    try {
                        Class<? extends CanalEntry.RowData> rowDataClass = rowData.getClass();
                        Field beforeColumnsField = rowDataClass.getDeclaredField("beforeColumns_");
                        beforeColumnsField.setAccessible(true);
                        beforeColumnsField.set(rowData, rowData.getAfterColumnsList());
                        Field afterColumnsField = rowDataClass.getDeclaredField("afterColumns_");
                        afterColumnsField.setAccessible(true);
                        afterColumnsField.set(rowData, Collections.unmodifiableList(Collections.emptyList()));
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        LOGGER.error("反射异常", e);
                    }
                });
                logicDeletedEntryWrapper.setAllRowDataList(logicDeletedRowDataList);
                logicDeletedEntryWrapperList.add(logicDeletedEntryWrapper);
                entryWrapper.getAllRowDataList().removeAll(logicDeletedRowDataList);
            }
        }
        wrapper.getEntryWrapperList().removeIf(entryWrapper -> entryWrapper.getAllRowDataList().isEmpty());
        wrapper.getEntryWrapperList().addAll(logicDeletedEntryWrapperList);
    }

    /**
     * aviator 执行
     *
     * @param columns               数据行的所有列
     * @param entryWrapper          数据
     * @param aviatorExpression     aviator 表达式
     * @param consumerConfigFactory ConsumerConfigFactory
     * @return true or false
     * @throws ExpressionSyntaxErrorException 表达式返回boolean类型, 否则抛出异常
     */
    public static boolean exec(List<CanalEntry.Column> columns, EntryWrapper entryWrapper,
                               String aviatorExpression, ConsumerConfigFactory consumerConfigFactory) {
        Expression expression = AviatorEvaluator.compile(aviatorExpression, true);
        ConsumerConfig consumerConfig = consumerConfigFactory.get(entryWrapper).orElse(null);
        Object execute = expression.execute(SqlTypeConverter.convertToActualType(consumerConfig,
                columns, entryWrapper.getSchemaName(), entryWrapper.getTableName(), true));
        if (execute instanceof Boolean) {
            return (boolean) execute;
        }
        throw new ExpressionSyntaxErrorException("表达式语法错误: " + aviatorExpression);
    }

    /**
     * 数据过滤
     *
     * @param entryWrapper          EntryWrapper
     * @param entryConsumerFactory  EntryConsumerFactory
     * @param consumerConfigFactory ConsumerConfigFactory
     */
    public static void filterEntryRowData(EntryWrapper entryWrapper, EntryConsumerFactory entryConsumerFactory,
                                          ConsumerConfigFactory consumerConfigFactory) {
        EntryConsumer<?> consumer = entryConsumerFactory.find(entryWrapper.getEventType()).orElse(null);
        // 如果不能处理, 置空, return
        if (consumer == null) {
            entryWrapper.setAllRowDataList(Collections.emptyList());
            return;
        }
        // 如果不可过滤, 跳过过滤
        if (!consumer.filterable()) {
            return;
        }
        consumerConfigFactory.get(entryWrapper)
                .map(ConsumerConfig::getFilter).ifPresent(o -> {
            if (!o.getEventTypes().contains(entryWrapper.getEventType())) {
                entryWrapper.setAllRowDataList(Collections.emptyList());
                return;
            }
            List<CanalEntry.RowData> rowDataList = entryWrapper.getAllRowDataList()
                    .stream()
                    .filter(rowData -> filterRowData(rowData, o, entryWrapper, consumerConfigFactory))
                    .collect(Collectors.toList());
            entryWrapper.setAllRowDataList(rowDataList);
        });
    }

    private static boolean filterRowData(CanalEntry.RowData rowData, Filter filter, EntryWrapper entryWrapper,
                                         ConsumerConfigFactory consumerConfigFactory) {
        if (!filterRowDataWithUpdatedFields(rowData, filter)) {
            return false;
        }
        String aviatorExpression = filter.getAviatorExpression();
        if (StringUtils.hasText(aviatorExpression)) {
            // 新增或者修改
            if (!CollectionUtils.isEmpty(rowData.getAfterColumnsList())) {
                return MessageUtils.exec(rowData.getAfterColumnsList(), entryWrapper, aviatorExpression,
                        consumerConfigFactory);
            }
            // 删除
            if (!CollectionUtils.isEmpty(rowData.getBeforeColumnsList())) {
                return MessageUtils.exec(rowData.getBeforeColumnsList(), entryWrapper, aviatorExpression,
                        consumerConfigFactory);
            }
        }
        return true;
    }

    private static boolean filterRowDataWithUpdatedFields(CanalEntry.RowData rowData, Filter filter) {
        List<String> updatedFields = filter.getUpdatedColumns();
        if (!CollectionUtils.isEmpty(updatedFields)) {
            Map<String, CanalEntry.Column> afterColumnMap = CommonUtils.toColumnMap(rowData.getAfterColumnsList());
            // 只考虑新增或者修改, 删除默认为已全部修改
            if (!CollectionUtils.isEmpty(afterColumnMap)) {
                Stream<Map.Entry<String, CanalEntry.Column>> stream = afterColumnMap.entrySet()
                        .stream()
                        .filter(entry -> updatedFields.contains(entry.getKey()));
                Map<String, CanalEntry.Column> beforeColumnMap =
                        CommonUtils.toColumnMap(rowData.getBeforeColumnsList());
                Predicate<Map.Entry<String, CanalEntry.Column>> predicate = entry -> {
                    CanalEntry.Column oldColumn = beforeColumnMap.get(entry.getKey());
                    return oldColumn == null || entry.getValue().getUpdated();
                };
                if (filter.isAnyUpdated()) {
                    return stream.anyMatch(predicate);
                }
                return stream.allMatch(predicate);
            }
        }
        return true;
    }
}
