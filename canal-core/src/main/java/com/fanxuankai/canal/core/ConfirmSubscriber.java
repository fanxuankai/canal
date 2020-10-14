package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.commons.util.concurrent.Flow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

/**
 * Canal 事务确认订阅者
 *
 * @author fanxuankai
 */
@Slf4j
public class ConfirmSubscriber implements Flow.Subscriber<MessageWrapper> {

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
        StopWatch sw = new StopWatch();
        sw.start();
        otter.getCanalConnector().ack(item.getBatchId());
        sw.stop();
        if (canalConfiguration.isShowEventLog() && !item.getEntryWrapperList().isEmpty()) {
            log.info("[" + canalConfiguration.getId() + "] " + "Confirm batchId: {} time: {}ms", item.getBatchId(),
                    sw.getTotalTimeMillis());
        }
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
