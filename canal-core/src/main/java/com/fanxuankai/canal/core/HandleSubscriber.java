package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.commons.util.concurrent.Flow;
import com.fanxuankai.commons.util.concurrent.SubmissionPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 处理订阅者
 *
 * @author fanxuankai
 */
@Slf4j
public class HandleSubscriber extends SubmissionPublisher<MessageWrapper>
        implements Flow.Subscriber<MessageWrapper> {

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
            log.info("[" + canalConfiguration.getId() + "] " + "Handle batchId: {} time: {}ms", item.getBatchId(),
                    sw.getTotalTimeMillis());
        }
        submit(item);
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
}
