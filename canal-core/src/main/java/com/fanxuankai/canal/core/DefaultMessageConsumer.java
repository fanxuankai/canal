package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.model.MessageWrapper;
import com.fanxuankai.canal.core.util.ConsumeEntryLogger;
import com.fanxuankai.canal.core.util.RedisKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StopWatch;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * Message 处理器
 *
 * @author fanxuankai
 */
public class DefaultMessageConsumer implements MessageConsumer {
    /**
     * logfile offset 标记后缀
     */
    protected static final String LOGFILE_OFFSET_SUFFIX = "LogfileOffset";
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageConsumer.class);
    private final CanalConfiguration canalConfiguration;
    private final RedisTemplate<String, Object> redisTemplate;
    private final EntryConsumerFactory entryConsumerFactory;
    private final ThreadPoolExecutor threadPoolExecutor;
    /**
     * logfile offset 消费标记
     */
    private final String logFileOffsetTag;

    public DefaultMessageConsumer(CanalConfiguration canalConfiguration, RedisTemplate<String, Object> redisTemplate,
                                  EntryConsumerFactory entryConsumerFactory, ThreadPoolExecutor threadPoolExecutor) {
        this.canalConfiguration = canalConfiguration;
        this.redisTemplate = redisTemplate;
        this.entryConsumerFactory = entryConsumerFactory;
        this.threadPoolExecutor = threadPoolExecutor;
        this.logFileOffsetTag = RedisKey.withPrefix("canal.serviceCache",
                canalConfiguration.getId() + Constants.SEPARATOR + LOGFILE_OFFSET_SUFFIX);
    }

    @Override
    public void accept(MessageWrapper messageWrapper) {
        if (CollectionUtils.isEmpty(messageWrapper.getEntryWrapperList())) {
            return;
        }
        try {
            if (messageWrapper.getRowDataCountAfterFilter() >= canalConfiguration.getPerformanceThreshold()) {
                doHandlePerformance(messageWrapper);
            } else {
                doHandle(messageWrapper);
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void doHandle(MessageWrapper messageWrapper) {
        for (EntryWrapper entryWrapper : messageWrapper.getEntryWrapperList()) {
            if (entryWrapper.getAllRowDataList().isEmpty()) {
                continue;
            }
            EntryConsumer<?> consumer =
                    entryConsumerFactory.find(entryWrapper.getEventType()).orElse(null);
            if (consumer == null || existsLogfileOffset(entryWrapper, messageWrapper.getBatchId())) {
                continue;
            }
            Object process = consumer.apply(entryWrapper);
            if (ObjectUtils.isEmpty(process)) {
                continue;
            }
            long time = consume(consumer, process, entryWrapper);
            logEntry(entryWrapper, messageWrapper.getBatchId(), time);
        }
    }

    @SuppressWarnings("rawtypes unchecked")
    private long consume(EntryConsumer consumer, Object process, EntryWrapper entryWrapper) {
        StopWatch sw = new StopWatch();
        sw.start();
        consumer.accept(process);
        sw.stop();
        long time = sw.getTotalTimeMillis();
        putOffset(entryWrapper.getLogfileName(), entryWrapper.getLogfileOffset());
        return time;
    }

    private void doHandlePerformance(MessageWrapper messageWrapper) throws ExecutionException, InterruptedException {
        // 异步处理
        List<Future<EntryWrapperProcess>> futureList = messageWrapper.getEntryWrapperList().stream()
                .map(entryWrapper -> threadPoolExecutor.submit(() -> {
                    EntryConsumer<?> consumer = entryConsumerFactory.find(entryWrapper.getEventType()).orElse(null);
                    if (consumer == null || existsLogfileOffset(entryWrapper, messageWrapper.getBatchId())) {
                        return new EntryWrapperProcess(entryWrapper, null, null);
                    }
                    return new EntryWrapperProcess(entryWrapper, consumer.apply(entryWrapper), consumer);
                }))
                .collect(Collectors.toList());
        // 顺序消费
        for (Future<EntryWrapperProcess> future : futureList) {
            EntryWrapperProcess entryWrapperProcess = future.get();
            Object process = entryWrapperProcess.process;
            if (!ObjectUtils.isEmpty(process)) {
                EntryWrapper entryWrapper = entryWrapperProcess.entryWrapper;
                EntryConsumer<?> consumer = entryWrapperProcess.consumer;
                long time = consume(consumer, process, entryWrapper);
                logEntry(entryWrapper, messageWrapper.getBatchId(), time);
            }
        }
    }

    private boolean existsLogfileOffset(EntryWrapper entryWrapper, long batchId) {
        String logfileName = entryWrapper.getLogfileName();
        long logfileOffset = entryWrapper.getLogfileOffset();
        // 开启了逻辑删除时，删除事件不防重消费
        if (canalConfiguration.isEnableLogicDelete()
                && entryWrapper.getEventType() == CanalEntry.EventType.DELETE) {
            return false;
        }
        if (existsLogfileOffset(logfileName, logfileOffset)) {
            LOGGER.info("防重消费 {} batchId: {}", entryWrapper.toString(), batchId);
            return true;
        }
        return false;
    }

    private void logEntry(EntryWrapper entryWrapper, long batchId, long time) {
        if (canalConfiguration.isShowEntryLog()) {
            ConsumeEntryLogger.LogInfo logInfo = new ConsumeEntryLogger.LogInfo();
            logInfo.setCanalConfiguration(canalConfiguration);
            logInfo.setEntryWrapper(entryWrapper);
            logInfo.setBatchId(batchId);
            logInfo.setTime(time);
            ConsumeEntryLogger.log(logInfo);
        }
    }

    private boolean existsLogfileOffset(String logfileName, long offset) {
        Object value = redisTemplate.opsForHash().get(logFileOffsetTag, logfileName);
        if (value == null) {
            return false;
        }
        return Long.parseLong(value.toString()) >= offset;
    }

    private void putOffset(String logfileName, long offset) {
        redisTemplate.opsForHash().put(logFileOffsetTag, logfileName, offset);
    }

    private static class EntryWrapperProcess {
        private final EntryWrapper entryWrapper;
        private final Object process;
        private final EntryConsumer<?> consumer;

        public EntryWrapperProcess(EntryWrapper entryWrapper, Object process, EntryConsumer<?> consumer) {
            this.entryWrapper = entryWrapper;
            this.process = process;
            this.consumer = consumer;
        }
    }

}
