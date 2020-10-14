package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.core.util.RedisKey;
import com.fanxuankai.commons.util.concurrent.ThreadPoolService;
import com.fanxuankai.commons.util.concurrent.Threads;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Canal 工作者
 *
 * @author fanxuankai
 */
@Slf4j
public class CanalWorker {

    @Getter
    private final CanalWorkConfiguration canalWorkConfiguration;
    private Otter otter;
    private volatile boolean running;

    public CanalWorker(CanalWorkConfiguration canalWorkConfiguration) {
        this.canalWorkConfiguration = canalWorkConfiguration;
        if (canalWorkConfiguration.getThreadPoolExecutor() == null) {
            canalWorkConfiguration.setThreadPoolExecutor(ThreadPoolService.getInstance());
        }
    }

    public void start() {
        if (running || !Objects.equals(canalWorkConfiguration.getCanalConfiguration().getEnabled(), Boolean.TRUE)) {
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
        log.info("[" + canalConfiguration.getId() + "] " + "ping...");
        RedisTemplate<String, Object> redisTemplate = canalWorkConfiguration.getRedisTemplate();
        if (Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(key, true,
                canalConfiguration.getPreemptive().getTimeout(), TimeUnit.SECONDS))) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> redisTemplate.delete(key)));
            log.info("[" + canalConfiguration.getId() + "] " + "pong...");
            canalWorkConfiguration.getThreadPoolExecutor().execute(() -> {
                do {
                    Threads.sleep(canalConfiguration.getPreemptive().getKeep(), TimeUnit.SECONDS);
                    redisTemplate.opsForValue().setIfPresent(key, true, canalConfiguration.getPreemptive().getTimeout(),
                            TimeUnit.SECONDS);
                } while (running);
            });
            otter.start();
        } else {
            Threads.sleep(canalConfiguration.getPreemptive().getPing(), TimeUnit.SECONDS);
            tryStart();
        }
    }
}
