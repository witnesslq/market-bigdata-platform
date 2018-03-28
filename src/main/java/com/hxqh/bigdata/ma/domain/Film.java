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
    private String category;

    public Film() {
    }

    public Film(Date addTime, Double numvalue, String name, String category) {
        this.addTime = addTime;
        this.numvalue = numvalue;
        this.name = name;
        this.category = category;
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

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
