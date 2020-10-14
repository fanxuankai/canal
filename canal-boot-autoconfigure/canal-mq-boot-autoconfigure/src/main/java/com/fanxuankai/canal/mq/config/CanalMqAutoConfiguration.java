package com.fanxuankai.canal.mq.config;

import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.mq.core.listener.ConsumerHelper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

/**
 * @author fanxuankai
 */
@ConditionalOnProperty(prefix = Constants.PREFIX + ".configuration", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalMqProperties.class)
@Import({AnnotationCanalListenerFactory.class, ConsumerHelper.class})
public class CanalMqAutoConfiguration {

}
