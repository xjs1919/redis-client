package com.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.xjs.redisclient.KV;
import com.github.xjs.redisclient.RedisClientService;
import com.test.entity.UserEntity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

import java.util.*;

@SpringBootTest
@RunWith(SpringRunner.class)
public class RedisTemplateServiceTest {

    @Autowired
    private RedisTemplate redisTemplate;


    @Test
    public void testSet(){
        UserEntity user = new UserEntity(1L, "xjs", "123456");
        redisTemplate.boundValueOps("user1").set(user);
    }




}
