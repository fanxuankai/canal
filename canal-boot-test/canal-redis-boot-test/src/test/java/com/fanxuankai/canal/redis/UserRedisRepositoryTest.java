package com.fanxuankai.canal.redis;

import com.alibaba.fastjson.JSON;
import com.fanxuankai.canal.redis.domain.User;
import com.fanxuankai.canal.redis.model.CombineKeyModel;
import com.fanxuankai.canal.redis.model.Entry;
import com.fanxuankai.canal.redis.model.UniqueKey;
import com.fanxuankai.canal.redis.model.UniqueKeyPro;
import com.fanxuankai.canal.redis.repository.UserRedisRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 * @author fanxuankai
 */
@SpringBootTest
@RunWith(SpringRunner.class)
@Slf4j
public class UserRedisRepositoryTest {
    @Resource
    private UserRedisRepository userRedisRepository;

    @Test
    public void findAllById() {
        log(userRedisRepository.findAllById(Arrays.asList(472546446994836472L, 472546446994836473L)));
    }

    @Test
    public void test() {
        log.info(JSON.toJSONString(userRedisRepository.findAll(), true));
        log.info(JSON.toJSONString(userRedisRepository.findOne(new UniqueKey("phone", "458361630032396291"))));
    }

    @Test
    public void findAll() {
        List<User> all = userRedisRepository.findAll();
        log.info("{}", JSON.toJSONString(all, true));
    }

    @Test
    public void testAll() {
        CombineKeyModel ck = new CombineKeyModel(Arrays.asList(new Entry("username", "jCplH5C")
                , new Entry("password", "rYR")));
        UniqueKey uk = new UniqueKey("phone", "F4Nh");
        UniqueKeyPro ukPro = new UniqueKeyPro("phone", Arrays.asList("QdWU", "cUsgBbeGN"));

        log.info("{}", userRedisRepository.count());
        log.info("{}", userRedisRepository.exists(uk));
        log(userRedisRepository.existsById(458353649886691328L));
        log(userRedisRepository.findById(458353649886691328L));
        userRedisRepository.findOne(uk).ifPresent(this::log);
        userRedisRepository.findOne(ck).ifPresent(this::log);
        log(userRedisRepository.findAll(ukPro));
        log(userRedisRepository.findAllById(Arrays.asList(458353649886691328L, 458353649895079936L)));
        userRedisRepository.findAll(Arrays.asList("username", "password")).forEach(this::log);
        userRedisRepository.findAll().forEach(this::log);
    }

    private void log(Object o) {
        log.info("{}", JSON.toJSONString(o, true));
    }
}
