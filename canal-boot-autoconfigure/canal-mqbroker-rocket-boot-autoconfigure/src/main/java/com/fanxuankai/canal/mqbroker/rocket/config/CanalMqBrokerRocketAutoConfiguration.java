package com.fanxuankai.canal.mqbroker.rocket.config;

import com.fanxuankai.boot.mqbroker.consume.EventListenerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.beans.factory.InitializingBean;

import javax.annotation.Resource;

/**
 * @author fanxuankai
 */
@Slf4j
public class CanalMqBrokerRocketAutoConfiguration implements InitializingBean {

    @Resource
    private DefaultMQPushConsumer consumer;

    @Override
    public void afterPropertiesSet() {
        EventListenerRegistry.getAllListenerMetadata()
                .forEach(s -> {
                    try {
                        consumer.unsubscribe(s.getTopic());
                        consumer.subscribe(s.getTopic(), "*");
                    } catch (MQClientException e) {
                        log.error("订阅失败", e);
                    }
                });
    }

}
