package com.github.xjs.redisclient;


import com.fasterxml.jackson.core.type.TypeReference;
import com.github.xjs.redisclient.key.ApplicationKeyPrefix;
import com.github.xjs.redisclient.key.KeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
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
        String realKey =  buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        redisTemplate.delete(keyBytes);
    }

    public boolean exists(KeyPrefix prefix, String key){
        return exists(true, prefix, key);
    }

    public boolean exists(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.hasKey(keyBytes);
    }

    public <T> T getSet(KeyPrefix prefix, String key, T value){
        return getSet(true, prefix, key, value);
    }

    public <T> T getSet(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, T value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
            String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
            String realKey = buildRealKey(enableAppKeyPrefix, prefix, k);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundValueOps(keyBytes).increment(offset);
    }

    /***************************HASH************************************/
    public void hset(KeyPrefix prefix, String key, String field, Object value){
        hset(true, prefix, key, field, value);
    }

    public void hset(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String field, Object value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        byte[] fieldBytes = field.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = (byte[])redisTemplate.boundHashOps(keyBytes).get(fieldBytes);
        return bytesToObject(valueBytes,valueClass);
    }

    public List<String> hkeys(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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
    public Long lpush(KeyPrefix prefix, String key, Object... values){
        return lpush(true, prefix, key, values);
    }

    public Long lpush(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object... values){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        if(values == null || values.length <= 0){
            return null;
        }
        byte[][] valueBytes = new byte[values.length][];
        for(int i=0;i<values.length;i++){
            valueBytes[i] = objectToBytes(values[i]);
        }
        return redisTemplate.boundListOps(keyBytes).leftPushAll(valueBytes);
    }

    public Long lpushx(KeyPrefix prefix, String key, Object value){
        return lpushx(true, prefix,key,value);
    }
    public Long lpushx(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundListOps(keyBytes).leftPushIfPresent(objectToBytes(value));
    }

    public <T> T lpop(KeyPrefix prefix, String key, Class<T> valueClass){
        return lpop(true, prefix, key, valueClass);
    }

    public <T> T lpop(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = redisTemplate.boundListOps(keyBytes).leftPop();
        if(valueBytes == null){
            return null;
        }
        return bytesToObject(valueBytes, valueClass);
    }


    public void lset(KeyPrefix prefix, String key, int index, Object value){
        lset(true, prefix, key, index, value);
    }

    public void lset(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int index, Object value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        redisTemplate.boundListOps(keyBytes).set(index, objectToBytes(value));
    }

    public <T> T lindex(KeyPrefix prefix, String key, int index, Class<T> valueClass){
        return lindex(true, prefix, key, index, valueClass);
    }

    public <T> T  lindex(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int index, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = redisTemplate.boundListOps(keyBytes).index(index);
        if(valueBytes == null){
          return null;
        }
        return bytesToObject(valueBytes, valueClass);
    }

    public int llen(KeyPrefix prefix, String key){
        return llen(true, prefix, key);
    }

    public int llen(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Long size = redisTemplate.boundListOps(keyBytes).size();
        if(size == null){
            return 0;
        }
        return size.intValue();
    }

    public <T> List<T> lrange(KeyPrefix prefix, String key, int start, int stop, Class<T> valueClass){
        return lrange(true, prefix, key, start, stop, valueClass);
    }

    public <T> List<T> lrange(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int start, int stop, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        List<byte[]> valueBytes = redisTemplate.boundListOps(keyBytes).range(start, stop);
        if(CollectionUtils.isEmpty(valueBytes)){
            return null;
        }
        return valueBytes.stream().map((v)->bytesToObject(v, valueClass)).collect(Collectors.toList());
    }

    public int lrem(KeyPrefix prefix, String key, int count, Object value){
        return lrem(true, prefix, key, count, value);
    }

    public int lrem(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int count, Object value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Long cnt = redisTemplate.boundListOps(keyBytes).remove(count, objectToBytes(value));
        return cnt == null ? 0 : cnt.intValue();
    }

    public void ltrim(KeyPrefix prefix, String key, int start, int stop){
        ltrim(true, prefix, key, start, stop);
    }

    public void ltrim(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int start, int stop){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        redisTemplate.boundListOps(keyBytes).trim(start, stop);
    }

    public Long rpush(KeyPrefix prefix, String key, Object... values){
        return rpush(true, prefix, key, values);
    }

    public Long rpush(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object... values){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        if(values == null || values.length <= 0){
            return null;
        }
        byte[][] valueBytes = new byte[values.length][];
        for(int i=0;i<values.length;i++){
            valueBytes[i] = objectToBytes(values[i]);
        }
        return redisTemplate.boundListOps(keyBytes).rightPushAll(valueBytes);
    }

    public Long rpushx(KeyPrefix prefix, String key, Object value){
        return rpushx(true, prefix,key,value);
    }
    public Long rpushx(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundListOps(keyBytes).rightPushIfPresent(objectToBytes(value));
    }

    public <T> T rpop(KeyPrefix prefix, String key, Class<T> valueClass){
        return rpop(true, prefix, key, valueClass);
    }

    public <T> T rpop(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = redisTemplate.boundListOps(keyBytes).rightPop();
        if(valueBytes == null){
            return null;
        }
        return bytesToObject(valueBytes, valueClass);
    }

    /***************************SET************************************/
    public Long sadd(KeyPrefix prefix, String key, Object... value){
        return sadd(true, prefix, key, value);
    }

    public Long sadd(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object... value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        if(value == null || value.length <= 0){
            return null;
        }
        byte[][] valuesBytes = new byte[value.length][];
        for(int i=0; i<value.length; i++){
            valuesBytes[i] = objectToBytes(value[i]);
        }
        return redisTemplate.boundSetOps(keyBytes).add(valuesBytes);
    }

    public <T> List<T> smembers(KeyPrefix prefix, String key, Class<T> valueClass){
        return smembers(true, prefix, key, valueClass);
    }

    public <T> List<T> smembers(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<byte[]> members = redisTemplate.boundSetOps(keyBytes).members();
        if(members == null || members.size() <= 0){
           return null;
        }
        return members.stream().map((v)->bytesToObject(v, valueClass)).collect(Collectors.toList());
    }

    public int scard(KeyPrefix prefix, String key){
        return scard(true, prefix, key);
    }

    public int scard(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Long size = redisTemplate.boundSetOps(keyBytes).size();
        return size==null?0:size.intValue();
    }

    public boolean sismember(KeyPrefix prefix, String key, Object value){
        return sismember(true, prefix, key, value);
    }

    public boolean sismember(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Boolean ret = redisTemplate.boundSetOps(keyBytes).isMember(objectToBytes(value));
        return ret==null?false:ret.booleanValue();
    }

    public <T> T srandmember(KeyPrefix prefix, String key, Class<T> valueClass){
        return srandmember(true, prefix, key, valueClass);
    }

    public <T> T srandmember(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Class<T> valueClass){
        List<T> retList = srandmember(enableAppKeyPrefix, prefix, key, 1, valueClass);
        if(retList == null || retList.size() <= 0){
            return null;
        }
        return retList.get(0);
    }

    public <T> List<T> srandmember(KeyPrefix prefix, String key, int count, Class<T> valueClass){
        return srandmember(true, prefix, key, count, valueClass);
    }

    public <T> List<T> srandmember(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int count, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<byte[]> values = redisTemplate.boundSetOps(keyBytes).distinctRandomMembers(count);
        if(values == null || values.size() <= 0){
            return null;
        }
        return values.stream().map((v)->bytesToObject(v, valueClass)).collect(Collectors.toList());
    }

    public Boolean srem(KeyPrefix prefix, String key, Object... values){
        return srem(true, prefix, key, values);
    }

    public Boolean srem(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object... values){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Long cnt = redisTemplate.boundSetOps(keyBytes).remove(objectsToBytes(values));
        if(cnt == null){
            return null;
        }
        return cnt.intValue() > 0;
    }

    public <T> T spop(KeyPrefix prefix, String key, Class<T> valueClass){
        return spop(true, prefix, key,  valueClass);
    }

    public <T> T spop(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Class<T> valueClass){
        List<T> ret = spop(enableAppKeyPrefix, prefix, key, 1, valueClass);
        if(CollectionUtils.isEmpty(ret)){
            return null;
        }
        return ret.get(0);
    }

    public <T> List<T> spop(KeyPrefix prefix, String key, int count, Class<T> valueClass){
        return spop(true, prefix, key, count, valueClass);
    }

    public <T> List<T> spop(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int count, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        List<byte[]> bytes = redisTemplate.opsForSet().pop(keyBytes, count);
        return bytesToObjects(bytes, valueClass);
    }

    public <T> Set<T> sscan(KeyPrefix prefix, String key, String pattern, Class<T> valueClass){
        return sscan(true, prefix, key, pattern, valueClass);
    }

    public <T> Set<T> sscan(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String pattern, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<T> set = new HashSet<T>();
        ScanOptions options = ScanOptions.scanOptions().count(10).match(pattern).build();
        Cursor<byte[]> cursor = redisTemplate.boundSetOps(keyBytes).scan(options);
        while(cursor.hasNext()){
            byte[] valueBytes = cursor.next();
            T t = bytesToObject(valueBytes, valueClass);
            set.add(t);
        }
        return set;
    }

    /***************************SortedSET************************************/
    public void zadd(KeyPrefix prefix, String key, ZSetOperations.TypedTuple... tuples){
        zadd(true, prefix, key, tuples);
    }
    public void zadd(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, ZSetOperations.TypedTuple... tuples){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        if(tuples == null || tuples.length <= 0){
            return;
        }
        Set<ZSetOperations.TypedTuple<byte[]>> valueBytes = new HashSet<>(tuples.length);
        for(ZSetOperations.TypedTuple tuple : tuples){
            ZSetOperations.TypedTuple t = new DefaultTypedTuple(objectToBytes(tuple.getValue()), tuple.getScore());
            valueBytes.add(t);
        }
        redisTemplate.boundZSetOps(keyBytes).add(valueBytes);
    }

    public <T> Set<T> zrange(KeyPrefix prefix, String key, double minScore, double maxScore, Class<T> valueClazz){
        return zrange(true, prefix, key, minScore, maxScore, valueClazz);
    }

    public <T> Set<T> zrange(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, double minScore, double maxScore, Class<T> valueClazz){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<byte[]> valueBytes = redisTemplate.boundZSetOps(keyBytes).rangeByScore(minScore, maxScore);
        if(valueBytes == null || valueBytes.size() <= 0){
            return null;
        }
        return valueBytes.stream().map((v)->bytesToObject(v, valueClazz)).collect(Collectors.toSet());
    }

    public <T> Set<ZSetOperations.TypedTuple<T>> zrangeWithScore(KeyPrefix prefix, String key, double minScore, double maxScore, Class<T> valueClazz){
        return zrangeWithScore(true, prefix, key, minScore, maxScore, valueClazz);
    }

    public <T> Set<ZSetOperations.TypedTuple<T>> zrangeWithScore(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, double minScore, double maxScore, Class<T> valueClazz){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<ZSetOperations.TypedTuple<byte[]>> valueBytes = redisTemplate.boundZSetOps(keyBytes).rangeByScoreWithScores(minScore, maxScore);
        if(valueBytes == null || valueBytes.size() <= 0){
            return null;
        }
        Set<ZSetOperations.TypedTuple<T>> ret = new HashSet<>(valueBytes.size());
        for(ZSetOperations.TypedTuple<byte[]> value : valueBytes){
            ZSetOperations.TypedTuple<T> tuple = new DefaultTypedTuple<>(bytesToObject(value.getValue(),valueClazz), value.getScore());
            ret.add(tuple);
        }
        return ret;
    }

    public Long zcard(KeyPrefix prefix, String key){
        return zcard(true, prefix, key);
    }

    public Long zcard(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).size();
    }

    public Long zcount(KeyPrefix prefix, String key, double minScore, double maxScore){
        return zcount(true, prefix, key, minScore, maxScore);
    }

    public Long zcount(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, double minScore, double maxScore){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).count(minScore, maxScore);
    }

    public Double zincrby(KeyPrefix prefix, String key, String member, double score){
        return zincrby(true, prefix, key, member, score);
    }
    public Double zincrby(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String member, double score){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).incrementScore(objectToBytes(member), score);
    }

    public Long zrank(KeyPrefix prefix, String key, String member){
        return zrank(true, prefix, key, member);
    }
    public Long zrank(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String member){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).rank(objectToBytes(member));
    }

    public Double zscore(KeyPrefix prefix, String key, String member){
        return zscore(true,prefix,key,member);
    }

    public Double zscore(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String member){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).score(objectToBytes(member));
    }

    public Long zrem(KeyPrefix prefix, String key, String... members){
        return zrem(true, prefix, key, members);
    }

    public Long zrem(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String... members){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).remove(objectsToBytes(members));
    }

    public Long zremByRank(KeyPrefix prefix, String key, int start, int stop){
        return zremByRank(true, prefix, key, start, stop);
    }

    public Long zremByRank(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int start, int stop){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).removeRange(start, stop);
    }

    public Long zremByScore(KeyPrefix prefix, String key, double min, double max){
        return zremByScore(true, prefix, key, min, max);
    }

    public Long zremByScore(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, double min, double max){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).removeRangeByScore(min, max);
    }

    public Long zrevRank(KeyPrefix prefix, String key, String member){
        return zrevRank(true, prefix, key, member);
    }

    public Long zrevRank(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String member){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        return redisTemplate.boundZSetOps(keyBytes).reverseRank(objectToBytes(member));
    }

    public <T>List<T> zrevRange(KeyPrefix prefix, String key, int start, int stop, Class<T> valueClass){
        return zrevRange(true, prefix, key, start, stop, valueClass);
    }

    public <T> List<T> zrevRange(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, int start, int stop, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<byte[]> values = redisTemplate.boundZSetOps(keyBytes).reverseRange(start, stop);
        if(values == null || values.size() <= 0){
            return null;
        }
        return bytesToObjects(values, valueClass);
    }

    public <T> List<T> zrevRangeByScore(KeyPrefix prefix, String key, double min, double max, Class<T> valueClass){
        return zrevRangeByScore(true, prefix, key, min, max, valueClass);
    }

    public <T> List<T> zrevRangeByScore(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, double min, double max, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<byte[]> values=redisTemplate.boundZSetOps(keyBytes).reverseRangeByScore(min,max);
        if(values == null || values.size() <= 0){
            return null;
        }
        return bytesToObjects(values, valueClass);
    }

    public <T> List<ZSetOperations.TypedTuple<T>> zrevRangeByScoreWithScore(KeyPrefix prefix, String key, double min, double max, Class<T> valueClass){
        return zrevRangeByScoreWithScore(true, prefix, key, min, max, valueClass);
    }

    public <T> List<ZSetOperations.TypedTuple<T>> zrevRangeByScoreWithScore(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, double min, double max, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<ZSetOperations.TypedTuple<byte[]>> values = redisTemplate.boundZSetOps(keyBytes).reverseRangeByScoreWithScores(min, max);
        if(values == null || values.size() <= 0){
            return null;
        }
        List<ZSetOperations.TypedTuple<T>> ret = new ArrayList<>(values.size());
        for(ZSetOperations.TypedTuple<byte[]> value : values){
            ZSetOperations.TypedTuple<T> tuple = new DefaultTypedTuple<>(bytesToObject(value.getValue(),valueClass), value.getScore());
            ret.add(tuple);
        }
        return ret;
    }

    public <T> Set<ZSetOperations.TypedTuple<T>> zscan(KeyPrefix prefix, String key, String pattern, Class<T> valueClass){
        return zscan(true, prefix, key, pattern, valueClass);
    }

    public <T> Set<ZSetOperations.TypedTuple<T>> zscan(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, String pattern, Class<T> valueClass){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        Set<ZSetOperations.TypedTuple<T>> set = new HashSet<>();
        ScanOptions options = ScanOptions.scanOptions().count(10).match(pattern).build();
        Cursor<ZSetOperations.TypedTuple<byte[]>> cursor = redisTemplate.boundZSetOps(keyBytes).scan(options);
        while(cursor.hasNext()){
            ZSetOperations.TypedTuple<byte[]> value = cursor.next();
            byte[] bytes = value.getValue();
            ZSetOperations.TypedTuple<T> tuple = new DefaultTypedTuple(bytesToObject(bytes, valueClass), value.getScore());
            set.add(tuple);
        }
        return set;
    }
    /***************************pub/sub************************************/
    public void publish(KeyPrefix prefix, String key, Object value){
        publish(true, prefix, key, value);
    }
    public void publish(boolean enableAppKeyPrefix, KeyPrefix prefix, String key, Object value){
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
        byte[] keyBytes = realKey.getBytes(StandardCharsets.UTF_8);
        redisTemplate.execute(connection -> {
            connection.publish(keyBytes, objectToBytes(value));
            return null;
        }, true);
    }

    /***************************lock/unlock************************************/
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
        String realKey = buildRealKey(enableAppKeyPrefix, prefix, key);
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

    private String buildRealKey(boolean enableAppKeyPrefix, KeyPrefix prefix, String key){
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

    private String buildKey(KeyPrefix keyPrefix, String key){
        if(keyPrefix == null){
            return key;
        }
        String prefix = keyPrefix.getPrefix();
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

    private byte[][] objectsToBytes(Object... values){
        if(values == null || values.length <= 0){
            return null;
        }
        byte[][] ret = new byte[values.length][];
        for(int i=0; i<values.length; i++){
            ret[i] = objectToBytes(values[i]);
        }
        return ret;
    }

    private <T> List<T> bytesToObjects(Collection<byte[]> val, Class<T> valueClazz){
        if(val == null || val.size() <= 0){
            return null;
        }
        List<T> ret = new ArrayList<>(val.size());
        for(Iterator<byte[]> it = val.iterator(); it.hasNext();){
            byte[] bytes = it.next();
            ret.add(bytesToObject(bytes, valueClazz));
        }
        return ret;
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

