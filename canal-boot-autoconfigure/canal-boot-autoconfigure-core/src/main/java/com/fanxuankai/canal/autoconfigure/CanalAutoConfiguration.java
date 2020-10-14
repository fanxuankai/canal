package com.fanxuankai.canal.autoconfigure;

import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.commons.util.concurrent.ThreadPoolService;
import org.springframework.beans.BeansException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.NonNull;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author fanxuankai
 */
public class CanalAutoConfiguration implements ApplicationContextAware, ApplicationRunner {

    private Collection<CanalWorker> canalWorkers = Collections.emptyList();

    @Resource
    private RedisTemplate<String, Object> redisTemplate;
    @Resource
    @Lazy
    private ThreadPoolExecutor threadPoolExecutor;

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public ThreadPoolExecutor threadPoolExecutor() {
        return ThreadPoolService.getInstance();
    }

    @Override
    public void run(ApplicationArguments args) {
        canalWorkers.stream()
                .peek(canalWorker -> canalWorker.getCanalWorkConfiguration()
                        .setRedisTemplate(redisTemplate)
                        .setThreadPoolExecutor(threadPoolExecutor))
                .forEach(CanalWorker::start);
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        canalWorkers = applicationContext.getBeansOfType(CanalWorker.class).values();
    }
}
