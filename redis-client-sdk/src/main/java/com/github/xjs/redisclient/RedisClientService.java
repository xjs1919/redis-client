package com.github.xjs.redisclient;


import com.fasterxml.jackson.core.type.TypeReference;
import com.github.xjs.redisclient.key.ApplicationKeyPrefix;
import com.github.xjs.redisclient.key.KeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisClientService {

    private RedisTemplate<byte[], byte[]> redisTemplate;
    private RedisClientProperties properties;
    private ApplicationKeyPrefix applicationKeyPrefix;

    public RedisClientService(RedisTemplate<byte[], byte[]> redisTemplate, RedisClientProperties properties, @Nullable ApplicationKeyPrefix applicationKeyPrefix){
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.applicationKeyPrefix = applicationKeyPrefix;
    }

    public <T> T get( KeyPrefix prefix, String key, Class<T> valueClazz){
        return get(true, prefix, key, valueClazz);
    }

    public <T> T get(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Class<T> valueClazz){
        String realKey =  buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        byte[] val = redisTemplate.boundValueOps(keyBytes).get();
        if(val == null){
            return null;
        }
        if(ClassUtils.isPrimitiveOrWrapper(valueClazz) || valueClazz == String.class){
            if(valueClazz == String.class){
                return (T)new String(val, StandardCharsets.UTF_8);
            }else if(valueClazz == int.class || valueClazz == Integer.class){
                return (T)Integer.valueOf(new String(val, StandardCharsets.UTF_8));
            }else if(valueClazz == long.class || valueClazz == Long.class){
                return (T)Long.valueOf(new String(val, StandardCharsets.UTF_8));
            }else if(valueClazz == boolean.class || valueClazz == Boolean.class){
                return (T)Boolean.valueOf(new String(val, StandardCharsets.UTF_8));
            }else if(valueClazz == byte.class || valueClazz == Byte.class){
                return (T)Byte.valueOf(new String(val, StandardCharsets.UTF_8));
            }else if(valueClazz == short.class || valueClazz == Short.class){
                return (T)Short.valueOf(new String(val, StandardCharsets.UTF_8));
            }else if(valueClazz == float.class || valueClazz == Float.class){
                return (T)Float.valueOf(new String(val, StandardCharsets.UTF_8));
            }else if(valueClazz == double.class || valueClazz == Double.class){
                return (T)Double.valueOf(new String(val, StandardCharsets.UTF_8));
            }else if(valueClazz == char.class || valueClazz == Character.class){
                return (T)Character.valueOf(new String(val, StandardCharsets.UTF_8).charAt(0));
            }else{
                return null;
            }
        }else if(valueClazz == byte[].class){
            return (T)val;
        } else if(valueClazz == TypeReference.class){
            return (T)((GenericJackson2JsonRedisSerializer)RedisSerializer.json()).deserialize(val, valueClazz);
        } else{
            return (T)RedisSerializer.json().deserialize(val);
        }
    }

    public Boolean set(KeyPrefix prefix, String key, Object value){
        return set(true, prefix, key, value, false);
    }

    public Boolean set(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object value){
        return set(enableAppKeyPrefix, prefix, key, value, false);
    }

    public Boolean set(KeyPrefix prefix, String key, Object value, boolean onlyNotExist){
        return set(true, prefix, key, value, onlyNotExist);
    }

    public Boolean set(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object value, boolean onlyNotExist){
        Class<?> clazz = value.getClass();
        byte[] val = null;
        if(ClassUtils.isPrimitiveOrWrapper(clazz) || clazz == String.class){
            val = value.toString().getBytes(StandardCharsets.UTF_8);
        }else if(clazz == byte[].class){
            val = (byte[])value;
        }else{
            val = RedisSerializer.json().serialize(value);
        }
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        int expireSeconds = prefix.getExpireSeconds();
        if(expireSeconds <= 0){
            if(onlyNotExist) {
                return redisTemplate.boundValueOps(keyBytes).setIfAbsent(val);
            }else {
                redisTemplate.boundValueOps(keyBytes).set(val);
                return true;
            }
        }else{
            if(onlyNotExist) {
                return redisTemplate.boundValueOps(keyBytes).setIfAbsent(val,  expireSeconds, TimeUnit.SECONDS);
            }else {
                redisTemplate.boundValueOps(keyBytes).set(val, expireSeconds, TimeUnit.SECONDS);
                return true;
            }
        }
    }

    public void delete(KeyPrefix prefix, String key){
        delete(true, prefix, key);
    }

    public void delete(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        redisTemplate.delete(keyBytes);
    }

    public boolean exists(KeyPrefix prefix, String key){
        return exists(true, prefix, key);
    }

    public boolean exists(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.hasKey(keyBytes);
    }

    public String lock(KeyPrefix prefix, String key, int waitSeconds){
        return lock(true, prefix, key, waitSeconds);
    }

    public String lock(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int waitSeconds){
        if(prefix.getExpireSeconds() <= 0){
            throw new IllegalArgumentException("分布式锁必须设置有效期");
        }
        int waitTimeout = waitSeconds * 1000;
        while (waitTimeout >= 0) {
            long expiresAt = System.currentTimeMillis() + prefix.getExpireSeconds()*1000 + 1;
            //锁到期时间
            String expiresAtStr = String.valueOf(expiresAt);
            //设置超时时间，防止客户端崩溃，锁得不到释放
            if (this.set(enableAppKeyPrefix, prefix, key, expiresAtStr, true)) {
                // lock acquired
                return expiresAtStr;
            }
            waitTimeout -= 100;
            try {
                Thread.sleep(100);
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public boolean unLock(KeyPrefix prefix, String key, String oldValue) {
        return unLock(true, prefix, key, oldValue);
    }

    public boolean unLock(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String oldValue) {
        //调用lua脚本
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        try {
            List<String> keys = new ArrayList<>();
            keys.add(realKey);
            Object result = executeScript(script, Long.class, keys, oldValue);
            if(result!=null && Integer.parseInt(result.toString()) == 1) {
                return true;
            }
            return false;
        } catch (final Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public RedisTemplate<byte[], byte[]> getRedisTemplate(){
        return this.redisTemplate;
    }

    private Object executeScript(String script, Class resultType, List<String> keys, String... args){
        List<byte[]> byteKeys = keys.stream().map((str)->str.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        int argsLength = (args==null||args.length<=0)?0:args.length;
        Object[] byteArgs = new Object[argsLength];
        for(int i=0; i<argsLength; i++){
            byteArgs[i] = args[i].getBytes(StandardCharsets.UTF_8);
        }
        return redisTemplate.execute(new DefaultRedisScript(script, resultType), byteKeys, byteArgs);
    }

    private String buildRealKey(boolean enableAppKeyPrefix, String prefix, String key){
        String appKeyPrefix = null;
        if(enableAppKeyPrefix && properties.isEnableApplicationKeyPrefix() && this.applicationKeyPrefix != null){
            appKeyPrefix = this.applicationKeyPrefix.getApplicationKeyPrefix();
        }
        if(StringUtils.isEmpty(appKeyPrefix)){
            return buildKey(prefix, key);
        }else{
            return appKeyPrefix +":"+ buildKey(prefix, key);
        }
    }

    private String buildKey(String prefix, String key){
        if(StringUtils.isEmpty(key)){
            if(prefix.endsWith(":")){
                return prefix.substring(0, prefix.length()-1);
            }
        }
        if(prefix.endsWith(":")){
            return prefix + key;
        }else{
            return prefix + ":"+ key;
        }
    }
}

