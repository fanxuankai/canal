package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.canal.core.util.Conversions;
import com.fanxuankai.commons.util.concurrent.Flow;
import com.fanxuankai.commons.util.concurrent.SubmissionPublisher;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * 消息转换订阅者
 *
 * @author fanxuankai
 */
public class ConvertProcessor extends SubmissionPublisher<MessageWrapper>
        implements Flow.Processor<Message, MessageWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertProcessor.class);
    private final Otter otter;
    private final CanalConfiguration canalConfiguration;
    private final ConsumerConfigFactory consumerConfigFactory;
    private Flow.Subscription subscription;

    public ConvertProcessor(Otter otter, CanalConfiguration canalConfiguration,
                            ConsumerConfigFactory consumerConfigFactory,
                            ThreadPoolExecutor threadPoolExecutor) {
        super(threadPoolExecutor, Flow.defaultBufferSize());
        this.otter = otter;
        this.canalConfiguration = canalConfiguration;
        this.consumerConfigFactory = consumerConfigFactory;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(Message item) {
        StopWatch sw = new StopWatch();
        sw.start();
        MessageWrapper wrapper = new MessageWrapper(item);
        logicDeleteConvert(wrapper);
        sw.stop();
        if (canalConfiguration.isShowEventLog() && !item.getEntries().isEmpty()) {
            LOGGER.info("[" + canalConfiguration.getId() + "] " + "Convert batchId: {} time: {}ms", item.getId(),
                    sw.getTotalTimeMillis());
        }
        submit(wrapper);
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

    private void logicDeleteConvert(MessageWrapper wrapper) {
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
                EntryWrapper logicDeletedEntryWrapper = new EntryWrapper(entryWrapper.getRaw());
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
}
