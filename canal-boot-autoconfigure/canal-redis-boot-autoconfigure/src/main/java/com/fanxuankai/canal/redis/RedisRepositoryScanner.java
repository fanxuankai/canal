package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.redis.repository.RedisRepository;
import com.fanxuankai.canal.redis.repository.SimpleRedisRepository;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.util.StopWatch;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
@Slf4j
@SuppressWarnings("rawtypes")
public class RedisRepositoryScanner {

    public static Set<Class<? extends RedisRepository>> scan(List<String> basePackages) {
        Reflections r =
                new Reflections(new ConfigurationBuilder()
                        .forPackages(basePackages.toArray(new String[0]))
                        .setScanners(new SubTypesScanner())
                );
        StopWatch sw = new StopWatch();
        sw.start();
        Set<Class<? extends RedisRepository>> redisRepositories = Collections.emptySet();
        try {
            redisRepositories = r.getSubTypesOf(RedisRepository.class)
                    .stream()
                    .filter(aClass -> !SimpleRedisRepository.class.isAssignableFrom(aClass))
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            log.warn(e.getLocalizedMessage());
        }
        sw.stop();
        String simpleName = RedisRepository.class.getSimpleName();
        log.info("Finished {} scanning in {}ms. Found {} {} interfaces.", simpleName, sw.getTotalTimeMillis(),
                redisRepositories.size(), simpleName);
        return redisRepositories;
    }

}
