package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.model.Filter;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.canal.core.util.CommonUtils;
import com.fanxuankai.canal.core.util.SqlTypeConverter;
import com.fanxuankai.commons.util.concurrent.Flow;
import com.fanxuankai.commons.util.concurrent.SubmissionPublisher;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 过滤订阅者
 *
 * @author fanxuankai
 */
public class FilterSubscriber extends SubmissionPublisher<MessageWrapper>
        implements Flow.Subscriber<MessageWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterSubscriber.class);

    private final Otter otter;
    private final CanalConfiguration canalConfiguration;
    private final ConsumerConfigFactory consumerConfigFactory;
    private final EntryConsumerFactory entryConsumerFactory;
    private Flow.Subscription subscription;

    public FilterSubscriber(Otter otter,
                            CanalConfiguration canalConfiguration,
                            ConsumerConfigFactory consumerConfigFactory,
                            EntryConsumerFactory entryConsumerFactory,
                            ThreadPoolExecutor threadPoolExecutor) {
        super(threadPoolExecutor, Flow.defaultBufferSize());
        this.otter = otter;
        this.canalConfiguration = canalConfiguration;
        this.consumerConfigFactory = consumerConfigFactory;
        this.entryConsumerFactory = entryConsumerFactory;
    }

    /**
     * aviator 执行
     *
     * @param columns           数据行的所有列
     * @param entryWrapper      数据
     * @param aviatorExpression aviator 表达式
     * @return true or false
     * @throws ExpressionSyntaxErrorException 表达式返回boolean类型, 否则抛出异常
     */
    private boolean exec(List<CanalEntry.Column> columns, EntryWrapper entryWrapper,
                         String aviatorExpression) {
        Expression expression = AviatorEvaluator.compile(aviatorExpression, true);
        ConsumerConfig consumerConfig = consumerConfigFactory.get(entryWrapper).orElse(null);
        Object execute = expression.execute(SqlTypeConverter.convertToActualType(consumerConfig,
                columns, entryWrapper.getSchemaName(), entryWrapper.getTableName(), true));
        if (execute instanceof Boolean) {
            return (boolean) execute;
        }
        throw new ExpressionSyntaxErrorException("表达式语法错误: " + aviatorExpression);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(MessageWrapper item) {
        if (!item.getEntryWrapperList().isEmpty()) {
            long batchId = item.getBatchId();
            StopWatch sw = new StopWatch();
            sw.start();
            item.getEntryWrapperList().forEach(this::filterEntryRowData);
            sw.stop();
            if (canalConfiguration.isShowEventLog()) {
                LOGGER.info("[" + canalConfiguration.getId() + "] " + "Filter batchId: {} rowDataCount: {} -> {} " +
                                "time: " +
                                "{}ms", batchId,
                        item.getRowDataCountBeforeFilter(),
                        item.getRowDataCountAfterFilter(),
                        sw.getTotalTimeMillis());
            }
        }
        submit(item);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("[" + canalConfiguration.getId() + "] " + throwable.getLocalizedMessage(), throwable);
        this.subscription.cancel();
        this.otter.stop();
    }

    @Override
    public void onComplete() {
        LOGGER.info("[" + canalConfiguration.getId() + "] " + "Done");
    }

    private void filterEntryRowData(EntryWrapper entryWrapper) {
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
                    .filter(rowData -> filterRowData(rowData, o, entryWrapper))
                    .collect(Collectors.toList());
            entryWrapper.setAllRowDataList(rowDataList);
        });
    }

    private boolean filterRowData(CanalEntry.RowData rowData, Filter filter, EntryWrapper entryWrapper) {
        if (!filterRowDataWithUpdatedFields(rowData, filter)) {
            return false;
        }
        String aviatorExpression = filter.getAviatorExpression();
        if (StringUtils.hasText(aviatorExpression)) {
            // 新增或者修改
            if (!CollectionUtils.isEmpty(rowData.getAfterColumnsList())) {
                return exec(rowData.getAfterColumnsList(), entryWrapper, aviatorExpression);
            }
            // 删除
            if (!CollectionUtils.isEmpty(rowData.getBeforeColumnsList())) {
                return exec(rowData.getBeforeColumnsList(), entryWrapper, aviatorExpression);
            }
        }
        return true;
    }

    private boolean filterRowDataWithUpdatedFields(CanalEntry.RowData rowData, Filter filter) {
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
