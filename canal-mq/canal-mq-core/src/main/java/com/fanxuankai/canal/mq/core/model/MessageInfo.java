package com.fanxuankai.canal.mq.core.model;

import java.util.List;

/**
 * @author fanxuankai
 */
public class MessageInfo {
    private String group;
    private String topic;
    private List<String> messages;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }
}
