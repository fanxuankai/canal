package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.util.CanalConnectorHelper;
import com.fanxuankai.commons.util.concurrent.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * Otter 客户端抽象类
 *
 * @author fanxuankai
 */
public abstract class AbstractOtter implements Otter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOtter.class);
    /**
     * 过滤的事件类型
     */
    private static final List<CanalEntry.EventType> EVENT_TYPES = Arrays.asList(INSERT, DELETE, UPDATE, ERASE);
    protected final CanalConfiguration canalConfiguration;
    private final CanalConnectorHelper canalConnectorHelper;
    private volatile boolean running;

    public AbstractOtter(CanalConfiguration canalConfiguration) {
        this.canalConfiguration = canalConfiguration;
        CanalConnectorHelper canalConnectorHelper = new CanalConnectorHelper();
        canalConnectorHelper.setDestination(canalConfiguration.getInstance());
        canalConnectorHelper.setFilter(canalConfiguration.getFilter());
        canalConnectorHelper.setUsername(canalConfiguration.getUsername());
        canalConnectorHelper.setPassword(canalConfiguration.getPassword());
        Optional.ofNullable(canalConfiguration.getCluster())
                .map(CanalConfiguration.Cluster::getNodes)
                .ifPresent(canalConnectorHelper::setZkServers);
        Optional.ofNullable(canalConfiguration.getSingleNode())
                .ifPresent(singleNode -> {
                    canalConnectorHelper.setHostname(singleNode.getHostname());
                    canalConnectorHelper.setPort(singleNode.getPort());
                });
        this.canalConnectorHelper = canalConnectorHelper;
    }

    @Override
    public void stop() {
        this.running = false;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        LOGGER.info("[" + canalConfiguration.getId() + "] " + "canal running...");
        running = true;
        canalConnectorHelper.afterPropertiesSet();
        while (running) {
            try {
                long start = System.currentTimeMillis();
                // 获取指定数量的数据
                Message message =
                        canalConnectorHelper.getCanalConnector().getWithoutAck(canalConfiguration.getBatchSize());
                long t = System.currentTimeMillis() - start;
                message.setEntries(filter(message.getEntries()));
                long batchId = message.getId();
                if (batchId != -1) {
                    if (canalConfiguration.isShowEventLog() && !message.getEntries().isEmpty()) {
                        LOGGER.info("[" + canalConfiguration.getId() + "] " + "Get batchId: {} time: {}ms", batchId, t);
                    }
                    onMessage(message);
                }
                Threads.sleep(canalConfiguration.getIntervalMillis(), TimeUnit.MILLISECONDS);
            } catch (CanalClientException e) {
                LOGGER.error(canalConfiguration.getId(), e);
                canalConnectorHelper.reconnect();
            } catch (Exception e) {
                running = false;
                LOGGER.info("[" + canalConfiguration.getId() + "] " + "Stop get data", e);
            }
        }
    }

    /**
     * 处理
     *
     * @param message 信息
     */
    protected abstract void onMessage(Message message);

    /**
     * 只消费增、删、改、删表事件，其它事件暂不支持且会被忽略
     *
     * @param entries CanalEntry.Entry
     */
    private List<CanalEntry.Entry> filter(List<CanalEntry.Entry> entries) {
        if (CollectionUtils.isEmpty(entries)) {
            return Collections.emptyList();
        }
        return entries.stream()
                .filter(entry -> entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONBEGIN)
                .filter(entry -> entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONEND)
                .filter(entry -> EVENT_TYPES.contains(entry.getHeader().getEventType()))
                .collect(Collectors.toList());
    }

    @Override
    public CanalConnector getCanalConnector() {
        return canalConnectorHelper.getCanalConnector();
    }
}
