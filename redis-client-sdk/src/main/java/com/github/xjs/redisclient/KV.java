package com.github.xjs.redisclient;

public class KV {

    private String k;
    private Object v;

    public KV() {

    }

    public KV(String k, Object v) {
        this.k = k;
        this.v = v;
    }

    @Override
    public String toString() {
        return "KV{" +
                "k='" + k + '\'' +
                ", v=" + v +
                '}';
    }

    public String getK() {
        return k;
    }

    public void setK(String k) {
        this.k = k;
    }

    public Object getV() {
        return v;
    }

    public void setV(Object v) {
        this.v = v;
    }
}
