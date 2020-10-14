package com.fanxuankai.canal.test.consumer;

import com.fanxuankai.canal.mq.core.annotation.CanalListener;
import com.fanxuankai.canal.mq.core.annotation.Delete;
import com.fanxuankai.canal.mq.core.annotation.Insert;
import com.fanxuankai.canal.mq.core.annotation.Update;
import com.fanxuankai.canal.test.domain.User;
import lombok.extern.slf4j.Slf4j;

/**
 * @author fanxuankai
 */
@Slf4j
@CanalListener(entityClass = User.class, waitRateSeconds = 1, waitMaxSeconds = 1)
public class UserCanalListener {

    @Update
    public void update(User before, User after) {
        log.info("update {}", before.getId());
    }

    @Insert
    public void insert(User user) {
        log.info("insert {}", user.getId());
    }

    @Delete
    public void delete(User user) {
        log.info("delete {}", user.getId());
    }
}
