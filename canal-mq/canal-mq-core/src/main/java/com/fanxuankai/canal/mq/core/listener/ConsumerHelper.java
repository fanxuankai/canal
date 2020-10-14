package com.fanxuankai.canal.mq.core.listener;

import com.fanxuankai.canal.core.constants.Constants;
import com.fanxuankai.canal.core.util.DomainConverter;
import com.fanxuankai.canal.core.util.Pair;
import com.fanxuankai.canal.mq.core.enums.EventType;

import java.util.Optional;
import java.util.function.BiConsumer;

import static com.fanxuankai.canal.mq.core.enums.EventType.*;

/**
 * @author fanxuankai
 */
public class ConsumerHelper {

    private final CanalListenerFactory canalListenerFactory;

    public ConsumerHelper(CanalListenerFactory canalListenerFactory) {
        this.canalListenerFactory = canalListenerFactory;
    }

    public void accept(BiConsumer<CanalListenerDefinition, String> queueConsumer) {
        canalListenerFactory.getAllCanalListener()
                .forEach(definition -> {
                    Optional.ofNullable(definition.getInsertConsumer())
                            .ifPresent(o -> queueConsumer.accept(definition,
                                    definition.getTopic() + Constants.SEPARATOR + INSERT));
                    Optional.ofNullable(definition.getUpdateConsumer())
                            .ifPresent(o -> queueConsumer.accept(definition,
                                    definition.getTopic() + Constants.SEPARATOR + UPDATE));
                    Optional.ofNullable(definition.getDeleteConsumer())
                            .ifPresent(o -> queueConsumer.accept(definition,
                                    definition.getTopic() + Constants.SEPARATOR + DELETE));
                });
    }

    public void consume(String topic, String msg) {
        consume(null, topic, msg);
    }

    public void consume(String group, String topic, String msg) {
        int i = topic.lastIndexOf(Constants.SEPARATOR);
        String rawTopic = topic.substring(0, i);
        EventType eventType = valueOf(topic.substring(i + 1));
        CanalListenerDefinition definition = Optional.ofNullable(group)
                .map(s -> canalListenerFactory.getCanalListenerGroupAndTopic(s, rawTopic))
                .orElse(canalListenerFactory.getCanalListenerByTopic(rawTopic));
        assert definition != null;
        if (eventType == INSERT && definition.getInsertConsumer() != null) {
            definition.getInsertConsumer().accept(DomainConverter.of(msg, definition.getEntityClass()));
        } else if (eventType == UPDATE && definition.getUpdateConsumer() != null) {
            Pair<Object, Object> pair = DomainConverter.pairOf(msg, definition.getEntityClass());
            definition.getUpdateConsumer().accept(pair.getKey(), pair.getValue());
        } else if (eventType == DELETE && definition.getDeleteConsumer() != null) {
            definition.getDeleteConsumer().accept(DomainConverter.of(msg, definition.getEntityClass()));
        }
    }
}
