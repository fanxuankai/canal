package com.fanxuankai.canal.test;

import com.alibaba.fastjson.JSON;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.listener.CanalListenerFactory;
import com.fanxuankai.canal.mq.core.listener.ConsumerHelper;
import com.fanxuankai.canal.mq.core.listener.SimpleCanalListenerFactory;
import com.fanxuankai.canal.test.consumer.UserCanalListener;
import com.fanxuankai.canal.test.domain.User;
import com.fanxuankai.canal.xxlmq.CanalXxlMqWorker;
import com.github.jsonzou.jmockdata.JMockData;

import java.util.Collections;

/**
 * @author fanxuankai
 */
public class CanalXxlMqDemo {
    public static void main(String[] args) {
        CanalWorker canalWorker = CanalXxlMqWorker.newCanalWorker(new CanalConfiguration()
                        .setInstance("canalMqExample")
                        .setFilter("canal_client_example.t_user")
                        .setShowEventLog(true)
                        .setShowEntryLog(true),
                new CanalMqConfiguration());
        canalWorker.getCanalWorkConfiguration()
                .setRedisTemplate(RedisTemplates.newRedisTemplate());
        canalWorker.start();

        // test consumer
        CanalListenerFactory canalListenerFactory =
                new SimpleCanalListenerFactory(Collections.singletonList(new UserCanalListener()));
        ConsumerHelper consumerHelper = new ConsumerHelper(canalListenerFactory);
        consumerHelper.consume("canal2Mq.canal_client_example.t_user.INSERT",
                JSON.toJSONString(JMockData.mock(User.class)));
    }
}