package com.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.xjs.redisclient.KV;
import com.github.xjs.redisclient.RedisClientService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class RedisClientServiceTest {

    @Autowired
    private RedisClientService redisService;

    @Test
    public void testSetObject(){
        redisService.set(UserKey.getById, ""+100,  new User(1, "xjs"));
    }

    @Test
    public void testGetObject(){
        User user = redisService.get(UserKey.getById, ""+100,  User.class);
        System.out.println(user);
    }

    @Test
    public void testSetSimple(){
        redisService.set(UserKey.simple, ""+100, 1);
    }

    @Test
    public void testGetSimple(){
        Integer i = redisService.get(UserKey.simple, ""+100,  Integer.class);
        System.out.println(i);
    }

    @Test
    public void testSetList(){
        List<User> users = new ArrayList<>();
        users.add(new User(1, "xjs"));
        users.add(new User(2, "aaa"));
        redisService.set(UserKey.list, ""+100,  users);
    }

    @Test
    public void testGetList(){
        TypeReference<List<User>> tf = new TypeReference<List<User>>(){};
        List<User> users = (List<User>)redisService.get(UserKey.list, ""+100, tf.getClass());
        System.out.println(users);
    }

    @Test
    public void testSetBytes(){
        redisService.set(UserKey.bytes, ""+100,  new byte[]{1,2,3,4,5});
    }

    @Test
    public void testGetBytes(){
        byte[] bytes = redisService.get(UserKey.bytes, ""+100,  byte[].class);
        for(int i=0; i<bytes.length; i++){
            System.out.println((int)bytes[i]);
        }
    }

    @Test
    public void testGetSet(){
        redisService.set(UserKey.getById, ""+100, "helloworld");
        String ret = redisService.getSet(UserKey.getById, ""+100, "java");
        System.out.println("old value:"+ret);
        String newvalue = redisService.get(UserKey.getById, ""+100, String.class);
        System.out.println("new value:"+newvalue);
    }

    @Test
    public void testMsetMget(){
        KV[] kvs = new KV[2];
        kvs[0] = new KV("hello", "world");
        kvs[1] = new KV("yes", "java");
        redisService.mset(UserKey.mset,kvs);
        List<String> values = redisService.mget(String.class, UserKey.mset, "hello", "yes");
        System.out.println(values);
    }

    @Test
    public void testIncr(){
        Long v = redisService.incr(UserKey.incr, "incr");
        System.out.println(v);
        redisService.incr(UserKey.incr, "incr", 10);
        redisService.incr(UserKey.incr, "incr", -5);
        int ret = redisService.get(UserKey.incr, "incr", Integer.class);
        System.out.println(ret);
    }

    @Test
    public void testLockUnlock(){
        String lockValue = redisService.lock(UserKey.lock, ""+100, 5);
        System.out.println("lockValue:" + lockValue);
        if(!StringUtils.isEmpty(lockValue)){
            boolean unlock = redisService.unLock(UserKey.lock, ""+100, lockValue);
            System.out.println("unlock:" + unlock);
        }
    }

    @Test
    public void testHash(){
        redisService.hset(UserKey.hkey1, ""+100, "username", "xjs");
        redisService.hset(UserKey.hkey1, ""+100, "password", new User(100, "hello"));
        String username = redisService.hget(UserKey.hkey1, ""+100, "username", String.class);
        User u = redisService.hget(UserKey.hkey1, ""+100, "password", User.class);
        System.out.println(username);
        System.out.println(u);
        int size = redisService.hlen(UserKey.hkey1, ""+100);
        System.out.println(size);
        List<String> keys = redisService.hkeys(UserKey.hkey1, ""+100);
        System.out.println(keys);
        redisService.hset(UserKey.hkey2, ""+100, "u1", new User(100, "hello"));
        redisService.hset(UserKey.hkey2, ""+100, "u2", new User(101, "world"));
        List<User> users = redisService.hvals(UserKey.hkey2, ""+100, User.class);
        System.out.println(users);
        Map<String, User> all = redisService.hgetall(UserKey.hkey2, ""+100,User.class);
        System.out.println(all);
        redisService.hdelete(UserKey.hkey2, ""+100, "u1", "u2");
        users = redisService.hvals(UserKey.hkey2, ""+100, User.class);
        System.out.println(users);
        System.out.println(redisService.hexists(UserKey.hkey2, ""+100, "u1"));
        System.out.println(redisService.hexists(UserKey.hkey1, ""+100, "username"));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("100", new User(100, "hello"));
        map.put("101", new User(101, "world"));
        redisService.hmset(UserKey.hkey3, ""+100, map);
        users = redisService.hmget(User.class,UserKey.hkey3, ""+100, "100", "101");
        System.out.println(users);
        List<String> scanKeys = redisService.hscanKeys(UserKey.hkey3, ""+100,"1*");
        System.out.println(scanKeys);
        Map<String, byte[]> scans = redisService.hscan(UserKey.hkey3, ""+100,"1*");
        for(Map.Entry<String, byte[]> entry : scans.entrySet()){
            System.out.println(entry.getKey() + "：" + ((GenericJackson2JsonRedisSerializer) RedisSerializer.json()).deserialize(entry.getValue(), User.class));
        }
    }

    public static class User{
        private int id;
        private String name;
        public User(){

        }
        public User(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }
        public void setId(int id) {
            this.id = id;
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

}
