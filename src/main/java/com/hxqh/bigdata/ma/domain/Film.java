package com.hxqh.bigdata.ma.domain;

import java.util.Date;

/**
 * Created by Ocean lin on 2018/3/19.
 *
 * @author Ocean lin
 */
public class Film {

    private Date addTime;
    private Double numvalue;
    private String name;

    public Film() {
    }

    public Film(Date addTime, Double numvalue, String name) {
        this.addTime = addTime;
        this.numvalue = numvalue;
        this.name = name;
    }

    public Date getAddTime() {
        return addTime;
    }

    public void setAddTime(Date addTime) {
        this.addTime = addTime;
    }

    public Double getNumvalue() {
        return numvalue;
    }

    public void setNumvalue(Double numvalue) {
        this.numvalue = numvalue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
