package com.github.xjs.redisclient;


import com.fasterxml.jackson.core.type.TypeReference;
import com.github.xjs.redisclient.key.ApplicationKeyPrefix;
import com.github.xjs.redisclient.key.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisClientService {

    private static Logger log = LoggerFactory.getLogger(RedisClientService.class);

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
        return bytesToObject(val, valueClazz);
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
        byte[] val = objectToBytes(value);
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

    public <T> T getSet(KeyPrefix prefix, String key, T value){
        return getSet(true, prefix, key, value);
    }

    public <T> T getSet(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, T value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        byte[] oldBytes = redisTemplate.boundValueOps(keyBytes).getAndSet(objectToBytes(value));
        if(oldBytes !=  null && oldBytes.length > 0){
            return (T)bytesToObject(oldBytes, value.getClass());
        }
        return null;
    }

    public <T> List<T> mget(Class<T> valueClass, KeyPrefix prefix, String... keys){
        return mget(true, valueClass, prefix, keys);
    }

    public <T> List<T> mget(boolean enableAppKeyPrefix, Class<T> valueClass, KeyPrefix prefix, String... keys){
        if(keys == null || keys.length <= 0){
            return null;
        }
        List<byte[]> byteKeys = new ArrayList<>();
        for(String key : keys){
            String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
            byteKeys.add(realKey.getBytes(StandardCharsets.UTF_8));
        }
        List<byte[]> valueBytes = redisTemplate.opsForValue().multiGet(byteKeys);
        if(valueBytes == null || valueBytes.size() <= 0){
            return null;
        }
        return valueBytes.stream().map((bytes)->bytesToObject(bytes, valueClass)).collect(Collectors.toList());
    }

    public void mset(KeyPrefix prefix, KV... kvs){
        mset(true, prefix, kvs);
    }

    public void mset(boolean enableAppKeyPrefix, KeyPrefix prefix, KV... kvs){
        if(kvs == null || kvs.length <= 0){
            return;
        }
        Map<byte[], byte[]> kvMap = new HashMap<byte[], byte[]>();
        for(KV kv : kvs){
            String k = kv.getK();
            Object v = kv.getV();
            String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), k);
            kvMap.put(realKey.getBytes(StandardCharsets.UTF_8), objectToBytes(v));
        }
        redisTemplate.opsForValue().multiSet(kvMap);
    }

    public Long incr(KeyPrefix prefix, String key){
        return incr(true, prefix, key, 1);
    }

    public Long incr(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        return incr(enableAppKeyPrefix, prefix, key, 1);
    }

    public Long incr(KeyPrefix prefix, String key, int offset){
        return incr(true, prefix, key, offset);
    }

    public Long incr(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int offset){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundValueOps(keyBytes).increment(offset);
    }

    /***************************HASH************************************/
    public void hset(KeyPrefix prefix, String key, String field, Object value){
        hset(true, prefix, key, field, value);
    }

    public void hset(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String field, Object value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        byte[] fieldBytes = field.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = objectToBytes(value);
        redisTemplate.boundHashOps(keyBytes).put(fieldBytes, valueBytes);
    }

    public <T> T hget(KeyPrefix prefix, String key, String field,  Class<T> memberValueClass){
        return hget(true, prefix, key, field, memberValueClass);
    }

    public List<String> hkeys(KeyPrefix prefix, String key){
        return hkeys(true, prefix, key);
    }

    public <T> T hget(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String field, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        byte[] fieldBytes = field.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = (byte[])redisTemplate.boundHashOps(keyBytes).get(fieldBytes);
        return bytesToObject(valueBytes,valueClass);
    }

    public List<String> hkeys(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<Object> keys = redisTemplate.boundHashOps(keyBytes).keys();
        if(keys == null || keys.size() <= 0){
            return null;
        }
        return keys.stream().map((k)->new String((byte[])k, StandardCharsets.UTF_8)).collect(Collectors.toList());
    }

    public <T> List<T> hvals(KeyPrefix prefix, String key, Class<T> valueClass){
        return hvals(true, prefix, key, valueClass);
    }

    public <T> List<T> hvals(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        List<Object> values = redisTemplate.boundHashOps(keyBytes).values();
        if(values == null || values.size() <= 0){
            return null;
        }
        return values.stream().map((v)->bytesToObject((byte[])v,valueClass)).collect(Collectors.toList());
    }

    public int hlen(KeyPrefix prefix, String key){
        return hlen(true, prefix, key);
    }

    public int hlen(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Long size = redisTemplate.boundHashOps(keyBytes).size();
        return size==null?0:size.intValue();
    }

    public void hdelete(KeyPrefix prefix, String key, String... fields){
        hdelete(true, prefix, key, fields);
    }

    public void hdelete(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String... fields){
        if(fields == null || fields.length <= 0){
            return;
        }
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Object[] fieldBytes = new Object[fields.length];
        for(int i=0; i<fields.length; i++){
            String field = fields[i];
            fieldBytes[i] = field.getBytes(StandardCharsets.UTF_8);
        }
        redisTemplate.boundHashOps(keyBytes).delete(fieldBytes);
    }

    public Boolean hexists(KeyPrefix prefix, String key, String field){
        return hexists(true, prefix, key, field);
    }

    public Boolean hexists(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String field){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundHashOps(keyBytes).hasKey(field.getBytes(StandardCharsets.UTF_8));
    }

    public <T> Map<String, T> hgetall(KeyPrefix prefix, String key, Class<T> valueClass){
        return hgetall(true, prefix, key, valueClass);
    }

    public <T> Map<String, T> hgetall(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Class<T> valueClass){
        List<String> fields =  this.hkeys(enableAppKeyPrefix, prefix, key);
        if(fields == null || fields.size() <= 0){
            return null;
        }
        Map<String, T> ret = new HashMap<String, T>();
        for(String field : fields){
            T value = hget(enableAppKeyPrefix, prefix, key, field, valueClass);
            if(value != null){
                ret.put(field, value);
            }
        }
        return ret;
    }

    public <T> List<T> hmget(Class<T> valueClass,KeyPrefix prefix, String key, String ...fields){
        return hmget(true, valueClass, prefix, key, fields);
    }

    public <T> List<T> hmget(boolean enableAppKeyPrefix, Class<T> valueClass,KeyPrefix prefix, String key, String ...fields){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        if(fields == null || fields.length <= 0){
            return null;
        }
        List<Object> fieldBytes = new ArrayList<>(fields.length);
        for(String field : fields){
            fieldBytes.add(field.getBytes(StandardCharsets.UTF_8));
        }
        List<Object> values = redisTemplate.boundHashOps(keyBytes).multiGet(fieldBytes);
        if(values == null || values.size() <= 0){
            return null;
        }
        return values.stream().map((v)->bytesToObject((byte[])v, valueClass)).collect(Collectors.toList());
    }

    public void hmset(KeyPrefix prefix, String key, Map<String, Object> fieldValues){
        hmset(true, prefix, key, fieldValues);
    }

    public void hmset(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Map<String, Object> fieldValues){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        if(fieldValues == null || fieldValues.size() <= 0){
            return;
        }
        Map<byte[], byte[]> bytes = new HashMap<>(fieldValues.size());
        for(Map.Entry<String, Object> entry : fieldValues.entrySet()){
            String k = entry.getKey();
            Object v = entry.getValue();
            bytes.put(k.getBytes(StandardCharsets.UTF_8), objectToBytes(v));
        }
        redisTemplate.boundHashOps(keyBytes).putAll(bytes);
    }

    public Map<String, byte[]> hscan(KeyPrefix prefix, String key, String pattern){
        return hscan(true, prefix, key, pattern);
    }

    public Map<String, byte[]> hscan(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String pattern){
        Map<String, byte[]> ret = new HashMap<>();
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        ScanOptions options = ScanOptions.scanOptions().count(10).match(pattern).build();
        Cursor<Map.Entry<Object, Object>> cursor = redisTemplate.boundHashOps(keyBytes).scan(options);
        while(cursor.hasNext()){
            Map.Entry<Object, Object> entry = cursor.next();
            byte[] k = (byte[])entry.getKey();
            byte[] v = (byte[])entry.getValue();
            ret.put(new String(k, StandardCharsets.UTF_8), v);
        }
        try{
            cursor.close();
        }catch(Exception e){
            log.error(e.getMessage(), e);
        }
        return ret;
    }

    public List<String> hscanKeys(KeyPrefix prefix, String key, String pattern){
        return hscanKeys(true, prefix, key, pattern);
    }

    public List<String> hscanKeys(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String pattern){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix.getPrefix(), key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        ScanOptions options = ScanOptions.scanOptions().count(10).match(pattern).build();
        Set<String> keys = new HashSet<>();
        Cursor<Map.Entry<Object, Object>> cursor = redisTemplate.boundHashOps(keyBytes).scan(options);
        while(cursor.hasNext()){
            Map.Entry<Object, Object> entry = cursor.next();
            byte[] k = (byte[])entry.getKey();
            keys.add(new String(k,StandardCharsets.UTF_8));
        }
        try{
            cursor.close();
        }catch(Exception e){
            log.error(e.getMessage(), e);
        }
        return new ArrayList<>(keys);
    }

    /***************************LIST************************************/




    /***************************SET************************************/


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

    private byte[] objectToBytes(Object value){
        Class clazz = value.getClass();
        if(ClassUtils.isPrimitiveOrWrapper(clazz) || clazz == String.class){
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }else if(clazz == byte[].class){
            return (byte[])value;
        }else{
            return RedisSerializer.json().serialize(value);
        }
    }

    private <T> T bytesToObject(byte[] val, Class<T> valueClazz){
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
}

