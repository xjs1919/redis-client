package com.test;

import com.github.xjs.redisclient.OnRedisMessageEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @EventListener
    public void event(OnRedisMessageEvent event){
        System.out.println(Thread.currentThread().getName()+":"+event.getChannel()+","+event.getValue());
    }
}