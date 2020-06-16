package com.github.xjs.redisclient;

import com.github.xjs.redisclient.key.ApplicationKeyPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;

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
        redisTemplate.setHashKeySerializer(null);
        redisTemplate.setHashValueSerializer(null);
        redisTemplate.setEnableDefaultSerializer(false);
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setBeanClassLoader(this.classLoader);
        redisTemplate.afterPropertiesSet();
        return new RedisClientService(redisTemplate, properties, appKeyPrefix);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value="spring.redis.enableApplicationKeyPrefix", havingValue="true", matchIfMissing = true)
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

    @Bean
    RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory connectionFactory, MessageListener messageListener) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(messageListener, new PatternTopic("*"));
        return container;
    }

    @Bean
    public MessageListener redisMessageListener(){
        return new DefaultMessageListener();
    }

    public static class DefaultMessageListener implements MessageListener, ApplicationContextAware {

        private ApplicationContext applicationContext;

        @Override
        public void onMessage(Message message, byte[] pattern) {
            byte[] channel = message.getChannel();
            byte[] body = message.getBody();
            applicationContext.publishEvent(new OnRedisMessageEvent(new String(channel, StandardCharsets.UTF_8), new String(body, StandardCharsets.UTF_8)));
        }

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException{
            this.applicationContext = applicationContext;
        }

    }

}
