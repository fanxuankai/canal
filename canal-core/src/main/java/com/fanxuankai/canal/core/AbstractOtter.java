package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.util.CanalConnectorHelper;
import com.fanxuankai.commons.util.concurrent.Threads;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

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
@Slf4j
public abstract class AbstractOtter implements Otter {

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
        log.info("[" + canalConfiguration.getId() + "] " + "canal running...");
        running = true;
        CanalConnector canalConnector = canalConnectorHelper.createConnect();
        while (running) {
            try {
                StopWatch sw = new StopWatch();
                sw.start();
                // 获取指定数量的数据
                Message message = canalConnector.getWithoutAck(canalConfiguration.getBatchSize());
                sw.stop();
                message.setEntries(filter(message.getEntries()));
                long batchId = message.getId();
                if (batchId != -1) {
                    if (canalConfiguration.isShowEventLog() && !message.getEntries().isEmpty()) {
                        log.info("[" + canalConfiguration.getId() + "] " + "Get batchId: {} time: {}ms", batchId,
                                sw.getTotalTimeMillis());
                    }
                    onMessage(message);
                }
                Threads.sleep(canalConfiguration.getIntervalMillis(), TimeUnit.MILLISECONDS);
            } catch (CanalClientException e) {
                log.error("[" + canalConfiguration.getId() + "] " + "canal 服务异常", e);
                canalConnectorHelper.reconnect();
            } catch (Exception e) {
                running = false;
                log.info("[" + canalConfiguration.getId() + "] " + "Stop get data", e);
            }
        }
        canalConnectorHelper.disconnect();
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
