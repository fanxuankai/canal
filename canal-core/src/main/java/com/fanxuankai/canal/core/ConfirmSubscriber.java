package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.commons.util.concurrent.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Canal 事务确认订阅者
 *
 * @author fanxuankai
 */
public class ConfirmSubscriber implements Flow.Subscriber<MessageWrapper> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfirmSubscriber.class);
    private final Otter otter;
    private final CanalConfiguration canalConfiguration;
    private Flow.Subscription subscription;

    public ConfirmSubscriber(Otter otter, CanalConfiguration canalConfiguration) {
        this.otter = otter;
        this.canalConfiguration = canalConfiguration;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(MessageWrapper item) {
        long start = System.currentTimeMillis();
        otter.getCanalConnector().ack(item.getBatchId());
        long t = System.currentTimeMillis() - start;
        if (canalConfiguration.isShowEventLog() && !item.getEntryWrapperList().isEmpty()) {
            LOGGER.info("[" + canalConfiguration.getId() + "] " + "Confirm batchId: {} time: {}ms", item.getBatchId()
                    , t);
        }
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
