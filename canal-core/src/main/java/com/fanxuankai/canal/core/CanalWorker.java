package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.core.util.RedisKey;
import com.fanxuankai.commons.util.concurrent.ThreadPoolService;
import com.fanxuankai.commons.util.concurrent.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.TimeUnit;

/**
 * Canal 工作者
 *
 * @author fanxuankai
 */
public class CanalWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalWorker.class);
    private final CanalWorkConfiguration canalWorkConfiguration;
    private Otter otter;
    private volatile boolean running;

    public CanalWorker(CanalWorkConfiguration canalWorkConfiguration) {
        this.canalWorkConfiguration = canalWorkConfiguration;
        if (canalWorkConfiguration.getThreadPoolExecutor() == null) {
            canalWorkConfiguration.setThreadPoolExecutor(ThreadPoolService.getInstance());
        }
    }

    public CanalWorkConfiguration getCanalWorkConfiguration() {
        return canalWorkConfiguration;
    }

    public void start() {
        if (running) {
            return;
        }
        this.otter = new FlowOtter(canalWorkConfiguration);
        canalWorkConfiguration.getThreadPoolExecutor().execute(this::tryStart);
        this.running = true;
    }

    public void stop() {
        otter.stop();
        this.running = false;
    }

    private void tryStart() {
        CanalConfiguration canalConfiguration = canalWorkConfiguration.getCanalConfiguration();
        String key = RedisKey.withPrefix("canal.serviceCache",
                canalConfiguration.getId() + Constants.SEPARATOR + "CanalRunning");
        RedisTemplate<String, Object> redisTemplate = canalWorkConfiguration.getRedisTemplate();
        LOGGER.info("[" + canalConfiguration.getId() + "] " + "ping...");
        do {
            if (Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(key, true,
                    canalConfiguration.getPreemptive().getTimeout(), TimeUnit.SECONDS))) {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> redisTemplate.delete(key)));
                LOGGER.info("[" + canalConfiguration.getId() + "] " + "pong...");
                canalWorkConfiguration.getThreadPoolExecutor().execute(() -> {
                    do {
                        Threads.sleep(canalConfiguration.getPreemptive().getKeep(), TimeUnit.SECONDS);
                        redisTemplate.opsForValue().set(key, true,
                                canalConfiguration.getPreemptive().getTimeout(), TimeUnit.SECONDS);
                    } while (running);
                });
                otter.start();
                break;
            }
            Threads.sleep(canalConfiguration.getPreemptive().getPing(), TimeUnit.SECONDS);
        } while (true);
    }
}
