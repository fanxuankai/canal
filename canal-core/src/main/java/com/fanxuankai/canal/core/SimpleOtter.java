package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.canal.core.util.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Otter 串行客户端
 *
 * @author fanxuankai
 */
public class SimpleOtter extends AbstractOtter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleOtter.class);

    private final CanalWorkConfiguration canalWorkConfiguration;

    public SimpleOtter(CanalWorkConfiguration canalWorkerConfiguration) {
        super(canalWorkerConfiguration.getCanalConfiguration());
        this.canalWorkConfiguration = canalWorkerConfiguration;
    }

    @Override
    protected void onMessage(Message message) {
        if (message.getEntries().isEmpty()) {
            return;
        }
        if (canalConfiguration.isSkip()) {
            try {
                getCanalConnector().ack(message.getId());
            } catch (CanalClientException e) {
                getCanalConnector().rollback(message.getId());
                LOGGER.error("[" + canalConfiguration.getId() + "] " + "Canal ack failure", e);
            }
        } else {
            long start = System.currentTimeMillis();
            MessageWrapper wrapper = new MessageWrapper(message);
            EntryConsumerFactory entryConsumerFactory = canalWorkConfiguration.getEntryConsumerFactory();
            ConsumerConfigFactory consumerConfigFactory = canalWorkConfiguration.getConsumerConfigFactory();
            wrapper.getEntryWrapperList().forEach(entryWrapper -> MessageUtils.filterEntryRowData(entryWrapper,
                    entryConsumerFactory, consumerConfigFactory));
            MessageConsumer messageConsumer = new DefaultMessageConsumer(canalConfiguration,
                    canalWorkConfiguration.getRedisTemplate(), entryConsumerFactory,
                    canalWorkConfiguration.getThreadPoolExecutor());
            try {
                messageConsumer.accept(wrapper);
                getCanalConnector().ack(wrapper.getBatchId());
            } catch (Exception e) {
                getCanalConnector().rollback(wrapper.getBatchId());
                LOGGER.error("[" + canalConfiguration.getId() + "] " + "Message consume failure", e);
            }
            if (canalConfiguration.isShowEventLog() && !wrapper.getEntryWrapperList().isEmpty()) {
                LOGGER.info("[" + canalConfiguration.getId() + "] " + "Handle batchId: {} time: {}ms",
                        wrapper.getBatchId(),
                        System.currentTimeMillis() - start);
            }
        }
    }

}
