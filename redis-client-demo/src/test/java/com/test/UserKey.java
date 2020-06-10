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
    public static UserKey mset = new UserKey("m:");
    public static UserKey incr = new UserKey("i:");
    public static UserKey lock = new UserKey("lk:", 10);
    public static UserKey hkey1 = new UserKey("hk1:");
    public static UserKey hkey2 = new UserKey("hk2:");
    public static UserKey hkey3 = new UserKey("hk3:");
    public static UserKey list1 = new UserKey("l1:");
    public static UserKey set = new UserKey("s1:");
    public static UserKey set2 = new UserKey("s2:");
}
