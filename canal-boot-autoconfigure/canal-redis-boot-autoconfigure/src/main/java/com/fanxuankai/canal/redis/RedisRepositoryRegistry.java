package com.fanxuankai.canal.redis;

import com.fanxuankai.canal.redis.repository.RedisRepository;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;

/**
 * @author fanxuankai
 */
public class RedisRepositoryRegistry {

    /**
     * 注册 bean
     *
     * @param registry     BeanDefinitionRegistry
     * @param basePackages the base packages
     */
    @SuppressWarnings("rawtypes")
    public static void registerWith(BeanDefinitionRegistry registry, List<String> basePackages) {
        Set<Class<? extends RedisRepository>> redisRepositories = RedisRepositoryScanner.scan(basePackages);
        if (CollectionUtils.isEmpty(redisRepositories)) {
            return;
        }
        redisRepositories.parallelStream()
                .forEach(redisRepository -> {
                    BeanDefinitionBuilder beanDefinitionBuilder =
                            BeanDefinitionBuilder.genericBeanDefinition(RedisRepositorySupport.class);
                    beanDefinitionBuilder.addConstructorArgValue(redisRepository);
                    registry.registerBeanDefinition(redisRepository.getName(),
                            beanDefinitionBuilder.getRawBeanDefinition());
                });
    }

}
