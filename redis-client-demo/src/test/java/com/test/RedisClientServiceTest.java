package com.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.xjs.redisclient.RedisClientService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

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
    public void testLockUnlock(){
        String lockValue = redisService.lock(UserKey.lock, ""+100, 5);
        System.out.println("lockValue:" + lockValue);
        if(!StringUtils.isEmpty(lockValue)){
            boolean unlock = redisService.unLock(UserKey.lock, ""+100, lockValue);
            System.out.println("unlock:" + unlock);
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
