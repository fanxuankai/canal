package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.commons.core.util.concurrent.Flow;
import com.fanxuankai.commons.core.util.concurrent.SubmissionPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * 处理订阅者
 *
 * @author fanxuankai
 */
public class HandleSubscriber extends SubmissionPublisher<MessageWrapper>
        implements Flow.Subscriber<MessageWrapper> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HandleSubscriber.class);
    private final Otter otter;
    private final CanalConfiguration canalConfiguration;
    private final MessageConsumer messageConsumer;
    private Flow.Subscription subscription;

    public HandleSubscriber(Otter otter, CanalConfiguration canalConfiguration, MessageConsumer messageConsumer,
                            ExecutorService executorService) {
        super(executorService, Flow.defaultBufferSize());
        this.otter = otter;
        this.canalConfiguration = canalConfiguration;
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(MessageWrapper item) {
        long start = System.currentTimeMillis();
        messageConsumer.accept(item);
        long t = System.currentTimeMillis() - start;
        if (Objects.equals(canalConfiguration.isShowEventLog(), Boolean.TRUE)
                && !item.getEntryWrapperList().isEmpty()) {
            LOGGER.info("[" + canalConfiguration.getId() + "] " + "Handle batchId: {} time: {}ms", item.getBatchId(),
                    t);
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
