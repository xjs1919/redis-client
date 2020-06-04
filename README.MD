# redis-client
对spring-boot-starter-data-redis的简单封装，更易于上手使用

## 存在的理由
- 1.统一key前缀管理，可以给key添加应用级别的前缀和类级别的前缀
- 2.统一key有效期管理
- 3.方便value的写入，对象以json个数写入，基本类型还是基本类型

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
}
```

## key前缀
- 1.sdk默认给key添加应用级别的前缀，默认是spring.application.name
- 2.可以注入实现了ApplicationKeyPrefix这个接口的类，提供应用级别的前缀
- 3.如果操作某个key不想添加ApplicationKeyPrefix，可以调用相关的api明确不使用应用前缀
- 4.如果整个应用都不想添加ApplicationKeyPrefix，可以设置spring.redis.enableApplicationKeyPrefix=false，默认是true。
