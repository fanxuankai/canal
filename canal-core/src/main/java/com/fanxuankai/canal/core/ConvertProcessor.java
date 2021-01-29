package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.protocol.Message;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.canal.core.util.MessageUtils;
import com.fanxuankai.commons.util.concurrent.Flow;
import com.fanxuankai.commons.util.concurrent.SubmissionPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

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
        long start = System.currentTimeMillis();
        MessageWrapper wrapper = new MessageWrapper(item, canalConfiguration);
        MessageUtils.logicDeleteConvert(wrapper, canalConfiguration, consumerConfigFactory);
        long t = System.currentTimeMillis() - start;
        if (canalConfiguration.isShowEventLog() && !item.getEntries().isEmpty()) {
            LOGGER.info("[" + canalConfiguration.getId() + "] " + "Convert batchId: {} time: {}ms", item.getId(), t);
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

}
