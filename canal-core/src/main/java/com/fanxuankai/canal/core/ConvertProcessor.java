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
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 消息转换订阅者
 *
 * @author fanxuankai
 */
@Slf4j
public class ConvertProcessor extends SubmissionPublisher<MessageWrapper>
        implements Flow.Processor<Message, MessageWrapper> {

    private final Otter otter;
    private final CanalConfiguration canalConfiguration;
    private final ConsumerConfigFactory consumerConfigFactory;
    private final EntryConsumerFactory entryConsumerFactory;
    private Flow.Subscription subscription;

    public ConvertProcessor(Otter otter, CanalConfiguration canalConfiguration,
                            ConsumerConfigFactory consumerConfigFactory,
                            EntryConsumerFactory entryConsumerFactory,
                            ThreadPoolExecutor threadPoolExecutor) {
        super(threadPoolExecutor, Flow.defaultBufferSize());
        this.otter = otter;
        this.canalConfiguration = canalConfiguration;
        this.consumerConfigFactory = consumerConfigFactory;
        this.entryConsumerFactory = entryConsumerFactory;
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
            log.info("[" + canalConfiguration.getId() + "] " + "Convert batchId: {} time: {}ms", item.getId(),
                    sw.getTotalTimeMillis());
        }
        submit(wrapper);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("[" + canalConfiguration.getId() + "] " + throwable.getLocalizedMessage(), throwable);
        this.subscription.cancel();
        this.otter.stop();
    }

    @Override
    public void onComplete() {
        log.info("[" + canalConfiguration.getId() + "] " + "Done");
    }

    private void logicDeleteConvert(MessageWrapper wrapper) {
        if (!canalConfiguration.isEnableLogicDelete()) {
            return;
        }
        for (EntryWrapper entryWrapper : wrapper.getEntryWrapperList()) {
            if (entryWrapper.getEventType() != CanalEntry.EventType.UPDATE) {
                continue;
            }
            EntryConsumer<?> consumer =
                    entryConsumerFactory.find(entryWrapper.getEventType()).orElse(null);
            if (consumer == null) {
                continue;
            }
            String logicDeleteField = consumerConfigFactory.get(entryWrapper)
                    .map(ConsumerConfig::getLogicDeleteField)
                    .orElse(null);
            if (!StringUtils.hasText(logicDeleteField)) {
                logicDeleteField = canalConfiguration.getLogicDeleteField();
            }
            boolean isLogicDelete = false;
            for (CanalEntry.RowData rowData : entryWrapper.getAllRowDataList()) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                for (CanalEntry.Column column : afterColumnsList) {
                    if (Objects.equals(logicDeleteField, column.getName())) {
                        isLogicDelete = Objects.equals(Conversions.getInstance().convert(column.getValue(),
                                Boolean.class), Boolean.TRUE);
                        break;
                    }
                }
                if (!isLogicDelete) {
                    break;
                }
            }
            if (isLogicDelete) {
                entryWrapper.setEventType(CanalEntry.EventType.DELETE);
                entryWrapper.getAllRowDataList().forEach(rowData -> {
                    try {
                        Class<? extends CanalEntry.RowData> rowDataClass = rowData.getClass();
                        Field beforeColumnsField = rowDataClass.getDeclaredField("beforeColumns_");
                        beforeColumnsField.setAccessible(true);
                        beforeColumnsField.set(rowData, rowData.getAfterColumnsList());
                        Field afterColumnsField = rowDataClass.getDeclaredField("afterColumns_");
                        afterColumnsField.setAccessible(true);
                        afterColumnsField.set(rowData, Collections.unmodifiableList(Collections.emptyList()));
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        log.error("反射异常", e);
                    }
                });
            }
        }
    }
}
