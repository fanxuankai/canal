package com.fanxuankai.canal.mq.core.model;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class MessageInfo {
    private String group;
    private String topic;
    private List<String> messages;
}
