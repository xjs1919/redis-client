package com.github.xjs.redisclient;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "redclient")
public class RedisClientProperties {

    private boolean enableApplicationKeyPrefix= true;

    public boolean isEnableApplicationKeyPrefix() {
        return enableApplicationKeyPrefix;
    }

    public void setEnableApplicationKeyPrefix(boolean enableApplicationKeyPrefix) {
        this.enableApplicationKeyPrefix = enableApplicationKeyPrefix;
    }
}
