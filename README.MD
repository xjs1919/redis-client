# redis-client
对spring-boot-starter-data-redis的简单封装，更易于上手使用

## 微信扫一扫关注公众号：爪哇优太儿
![扫一扫加关注](https://img-blog.csdnimg.cn/20190524100820287.jpg?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2dvbGRlbmZpc2gxOTE5,size_16,color_FFFFFF,t_7)

## 存在的理由
- 1.统一key前缀管理，可以给key添加应用级别的前缀和类级别的前缀
- 2.统一key有效期管理
- 3.方便value的写入，对象以json形式写入，基本类型还是基本类型
- 4.之前的RedisTemplate和StringRedisTemplate依旧照常使用
- 5.更方便的分布式锁和发布订阅

## 接入步骤
- 1.下载源码
本地执行：mvn clean install -DskipTests 安装依赖
- 2.添加依赖
```xml
<dependency>
	<groupId>com.github.xjs</groupId>
	<artifactId>redis-client-sdk</artifactId>
	<version>1.0.0</version>
</dependency>
```

- 3.添加redis的配置,跟之前spring-boot-starter-data-redis的配置是一样的
```yml
spring:
  redis:
    host: 192.168.1.201
    port: 6379
  application:
    name: demo
```

- 4.注入RedisClientService
```java
//举个例子
@Autowired
private RedisClientService redisService;
@Test
public void testSetObject(){
	redisService.set(UserKey.getById, ""+100,  new User(1, "xjs"));
	User user = redisService.get(UserKey.getById, ""+100,  User.class);
        System.out.println(user);
	
	redisService.set(UserKey.simple, ""+100, 1);
	Integer i = redisService.get(UserKey.simple, ""+100,  Integer.class);
        System.out.println(i);
	
	List<User> users = new ArrayList<>();
        users.add(new User(1, "xjs"));
        users.add(new User(2, "aaa"));
        redisService.set(UserKey.list, ""+100,  users);
	TypeReference<List<User>> tf = new TypeReference<List<User>>(){};
        List<User> users = (List<User>)redisService.get(UserKey.list, ""+100, tf.getClass());
        System.out.println(users);
}
```

## key前缀
- 1.sdk默认给key添加应用级别的前缀，默认是spring.application.name
- 2.可以注入实现了ApplicationKeyPrefix这个接口的类，提供应用级别的前缀
- 3.如果操作某个key不想添加ApplicationKeyPrefix，可以调用相关的api明确不使用应用前缀
- 4.如果整个应用都不想添加ApplicationKeyPrefix，可以设置spring.redis.enableApplicationKeyPrefix=false，默认是true。
