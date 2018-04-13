package com.hxqh.bigdata.ma.model.base;

/**
 * Created by Ocean lin on 2018/4/8.
 *
 * @author Ocean lin
 */
public class Message {

    private int code;
    private String message;

    public Message() {
    }

    public Message(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
