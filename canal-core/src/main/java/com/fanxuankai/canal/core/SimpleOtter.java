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
    private final MessageConsumer messageConsumer;

    public SimpleOtter(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration.getCanalConfiguration());
        this.canalWorkConfiguration = canalWorkConfiguration;
        this.messageConsumer = new DefaultMessageConsumer(canalConfiguration,
                canalWorkConfiguration.getRedisTemplate(),
                canalWorkConfiguration.getEntryConsumerFactory(),
                canalWorkConfiguration.getThreadPoolExecutor());
    }

    @Override
    protected void onMessage(Message message) {
        boolean isEntriesEmpty = message.getEntries().isEmpty();
        boolean showEventLog = canalConfiguration.isShowEventLog();
        long start, t;

        if (isEntriesEmpty || canalConfiguration.isSkip()) {
            try {
                getCanalConnector().ack(message.getId());
            } catch (CanalClientException e) {
                getCanalConnector().rollback(message.getId());
                LOGGER.error("[" + canalConfiguration.getId() + "] " + "Canal ack failure", e);
            }
            return;
        }

        // convert
        start = System.currentTimeMillis();
        MessageWrapper wrapper = new MessageWrapper(message);
        MessageUtils.logicDeleteConvert(wrapper, canalConfiguration, canalWorkConfiguration.getConsumerConfigFactory());
        t = System.currentTimeMillis() - start;
        if (showEventLog) {
            LOGGER.info("[" + canalConfiguration.getId() + "] " + "Convert batchId: {} time: {}ms",
                    wrapper.getBatchId(), t);
        }

        // filter
        start = System.currentTimeMillis();
        wrapper.getEntryWrapperList().forEach(entryWrapper -> MessageUtils.filterEntryRowData(entryWrapper,
                canalWorkConfiguration.getEntryConsumerFactory(), canalWorkConfiguration.getConsumerConfigFactory()));
        t = System.currentTimeMillis() - start;
        if (showEventLog) {
            LOGGER.info("[" + canalConfiguration.getId() + "] " + "Filter batchId: {} rowDataCount: {} -> {} " +
                            "time: {}ms", wrapper.getBatchId(), wrapper.getRowDataCountBeforeFilter(),
                    wrapper.getRowDataCountAfterFilter(), t);
        }

        // handle
        start = System.currentTimeMillis();
        messageConsumer.accept(wrapper);
        t = System.currentTimeMillis() - start;
        if (showEventLog) {
            LOGGER.info("[" + canalConfiguration.getId() + "] " + "Handle batchId: {} time: {}ms",
                    wrapper.getBatchId(), t);
        }

        // confirm
        try {
            start = System.currentTimeMillis();
            getCanalConnector().ack(wrapper.getBatchId());
            t = System.currentTimeMillis() - start;
            if (showEventLog) {
                LOGGER.info("[" + canalConfiguration.getId() + "] " + "Confirm batchId: {} time: {}ms",
                        wrapper.getBatchId(), t);
            }
        } catch (Exception e) {
            getCanalConnector().rollback(wrapper.getBatchId());
            LOGGER.error("[" + canalConfiguration.getId() + "] " + "Message consume failure", e);
        }
    }

}
