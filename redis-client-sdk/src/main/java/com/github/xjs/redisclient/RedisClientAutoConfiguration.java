package com.github.xjs.redisclient;

import com.github.xjs.redisclient.key.ApplicationKeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.StringUtils;
import reactor.util.annotation.Nullable;

@Configuration
@EnableConfigurationProperties(RedisClientProperties.class)
public class RedisClientAutoConfiguration implements EnvironmentAware, BeanClassLoaderAware {

    private static Logger log = LoggerFactory.getLogger(RedisClientAutoConfiguration.class);

    private Environment environment;
    private ClassLoader classLoader;

    @Bean
    public RedisClientService redisService(RedisConnectionFactory redisConnectionFactory, RedisClientProperties properties, @Nullable ApplicationKeyPrefix appKeyPrefix){
        RedisTemplate<byte[], byte[]> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(null);
        redisTemplate.setValueSerializer(null);
        redisTemplate.setEnableDefaultSerializer(false);
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setBeanClassLoader(this.classLoader);
        redisTemplate.afterPropertiesSet();
        return new RedisClientService(redisTemplate, properties, appKeyPrefix);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value="redclient.enableApplicationKeyPrefix", havingValue="true", matchIfMissing = true)
    public ApplicationKeyPrefix applicationKeyPrefix(){
        String appName = environment.getProperty("spring.application.name", String.class);
        if(StringUtils.isEmpty(appName)){
            log.warn("无法获取应用的名字，key的应用前缀为空，有可能会与别的项目冲突");
        }
        return () -> appName;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }
}
