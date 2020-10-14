package com.fanxuankai.canal.xxlmq;

import com.fanxuankai.canal.mq.core.listener.ConsumerHelper;
import com.xxl.mq.client.consumer.IMqConsumer;
import com.xxl.mq.client.consumer.MqResult;
import com.xxl.mq.client.consumer.annotation.MqConsumer;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public class XxlMqConsumer implements IMqConsumer {

    private final MqConsumer mqConsumer;
    private final ConsumerHelper consumerHelper;

    public XxlMqConsumer(ConsumerHelper consumerHelper) {
        this.mqConsumer = AnnotationUtils.findAnnotation(getClass(), MqConsumer.class);
        this.consumerHelper = consumerHelper;
    }

    @Override
    public MqResult consume(String s) {
        consumerHelper.consume(Optional.of(mqConsumer.group())
                .filter(StringUtils::hasText)
                .filter(group -> !Objects.equals(MqConsumer.DEFAULT_GROUP, group))
                .orElse(null), mqConsumer.topic(), s);
        return MqResult.SUCCESS;
    }
}
