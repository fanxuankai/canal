package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.canal.core.util.MessageUtils;
import com.fanxuankai.commons.core.util.concurrent.Flow;
import com.fanxuankai.commons.core.util.concurrent.SubmissionPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

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
                            ExecutorService executorService) {
        super(executorService, Flow.defaultBufferSize());
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
    public void onNext(MessageWrapper item) {
        if (!item.getEntryWrapperList().isEmpty()) {
            long batchId = item.getBatchId();
            long start = System.currentTimeMillis();
            item.getEntryWrapperList().forEach(entryWrapper -> MessageUtils.filterEntryRowData(entryWrapper,
                    entryConsumerFactory, consumerConfigFactory));
            long t = System.currentTimeMillis() - start;
            if (canalConfiguration.isShowEventLog()) {
                LOGGER.info("[" + canalConfiguration.getId() + "] " + "Filter batchId: {} rowDataCount: {} -> {} " +
                                "time: {}ms", batchId, item.getRowDataCountBeforeFilter(),
                        item.getRowDataCountAfterFilter(), t);
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

}
