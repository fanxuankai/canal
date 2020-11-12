package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.commons.util.concurrent.Flow;
import com.fanxuankai.commons.util.concurrent.SubmissionPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

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
                            ThreadPoolExecutor threadPoolExecutor) {
        super(threadPoolExecutor, Flow.defaultBufferSize());
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
        StopWatch sw = new StopWatch();
        sw.start();
        messageConsumer.accept(item);
        sw.stop();
        if (Objects.equals(canalConfiguration.isShowEventLog(), Boolean.TRUE)
                && !item.getEntryWrapperList().isEmpty()) {
            LOGGER.info("[" + canalConfiguration.getId() + "] " + "Handle batchId: {} time: {}ms", item.getBatchId(),
                    sw.getTotalTimeMillis());
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
