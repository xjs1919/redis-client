package com.test;


import com.github.xjs.redisclient.key.AbstractKey;

public class UserKey extends AbstractKey {

    public UserKey(String value) {
        super(value);
    }

    public UserKey(String value, int expireSeconds) {
        super(value, expireSeconds);
    }

    public static UserKey getById = new UserKey("id:");
    public static UserKey simple = new UserKey("s:");
    public static UserKey list = new UserKey("l:");
    public static UserKey bytes = new UserKey("b:");
    public static UserKey lock = new UserKey("lk:", 10);
}
