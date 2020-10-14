package com.fanxuankai.canal.mq.core.consumer;

import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.config.ConsumerConfigSupplier;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.CommonUtils;
import com.fanxuankai.canal.mq.core.model.MessageInfo;

import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public interface UpdateFunction extends MessageFunction, ConsumerConfigSupplier {

    /**
     * insert 事件数据转换
     *
     * @param entryWrapper 数据
     * @return MessageInfo
     */
    @Override
    default MessageInfo apply(EntryWrapper entryWrapper) {
        String schemaName = entryWrapper.getSchemaName();
        String tableName = entryWrapper.getTableName();
        ConsumerConfig consumerConfig = getConsumerConfig(entryWrapper);
        return new MessageInfo()
                .setGroup(getGroup(entryWrapper))
                .setTopic(getTopic(entryWrapper))
                .setMessages(entryWrapper.getAllRowDataList()
                        .stream()
                        .map(rowData -> CommonUtils.jsonWithActualType(consumerConfig,
                                rowData.getBeforeColumnsList(), rowData.getAfterColumnsList(),
                                schemaName, tableName, false))
                        .collect(Collectors.toList()));
    }
}
