package com.github.xjs.redisclient;

import org.springframework.context.ApplicationEvent;

public class OnRedisMessageEvent extends ApplicationEvent {

    private String channel;
    private String value;

    public OnRedisMessageEvent(Object source, String value) {
        super(source);
        this.channel = (String)source;
        this.value = value;
    }

    public String getChannel() {
        return channel;
    }

    public String getValue() {
        return value;
    }
}
