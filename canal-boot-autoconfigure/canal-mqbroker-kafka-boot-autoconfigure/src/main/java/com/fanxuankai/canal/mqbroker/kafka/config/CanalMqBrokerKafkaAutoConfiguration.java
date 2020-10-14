package com.fanxuankai.canal.mqbroker.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author fanxuankai
 */
@Slf4j
public class CanalMqBrokerKafkaAutoConfiguration implements InitializingBean {

    @Override
    public void afterPropertiesSet() {
        // todo KafkaMessageListenerContainer 配置
    }

}
