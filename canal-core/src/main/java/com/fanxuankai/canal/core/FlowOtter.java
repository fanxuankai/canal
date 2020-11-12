package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.commons.util.concurrent.Flow;
import com.fanxuankai.commons.util.concurrent.SubmissionPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Otter 并行流客户端
 *
 * @author fanxuankai
 */
public class FlowOtter extends AbstractOtter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowOtter.class);
    private final SubmissionPublisher<Message> publisher;

    public FlowOtter(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration.getCanalConfiguration());
        ThreadPoolExecutor threadPoolExecutor = canalWorkConfiguration.getThreadPoolExecutor();
        publisher = new SubmissionPublisher<>(threadPoolExecutor, Flow.defaultBufferSize());
        EntryConsumerFactory entryConsumerFactory = canalWorkConfiguration.getEntryConsumerFactory();
        MessageConsumer messageConsumer = new DefaultMessageConsumer(canalConfiguration,
                canalWorkConfiguration.getRedisTemplate(), entryConsumerFactory, threadPoolExecutor);
        ConsumerConfigFactory consumerConfigFactory = canalWorkConfiguration.getConsumerConfigFactory();
        ConvertProcessor convertProcessor = new ConvertProcessor(this, canalConfiguration, consumerConfigFactory,
                entryConsumerFactory, threadPoolExecutor);
        FilterSubscriber filterSubscriber = new FilterSubscriber(this, canalConfiguration, consumerConfigFactory,
                entryConsumerFactory, threadPoolExecutor);
        HandleSubscriber handleSubscriber = new HandleSubscriber(this, canalConfiguration, messageConsumer,
                threadPoolExecutor);
        ConfirmSubscriber confirmSubscriber = new ConfirmSubscriber(this, canalConfiguration);
        publisher.subscribe(convertProcessor);
        convertProcessor.subscribe(filterSubscriber);
        filterSubscriber.subscribe(handleSubscriber);
        handleSubscriber.subscribe(confirmSubscriber);
    }

    @Override
    protected void onMessage(Message message) {
        if (canalConfiguration.isSkip()) {
            try {
                getCanalConnector().ack(message.getId());
            } catch (CanalClientException e) {
                getCanalConnector().rollback(message.getId());
                LOGGER.error("[" + canalConfiguration.getId() + "] " + "Canal ack failure", e);
            }
        } else {
            publisher.submit(message);
        }
    }

    @Override
    public void stop() {
        super.stop();
        publisher.close();
    }
}
