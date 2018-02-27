package com.hxqh.bigdata.ma.domain;

/**
 * Created by Ocean lin on 2018/2/27.
 *
 * @author Ocean lin
 */
public class Book {

    private String source;
    private String bookName;
    private String category;
    private String categoryLable;
    private Float price;
    private Long commnetNum;
    private String author;
    private String publish;

    public Book() {
    }

    public Book(String source, String bookName, String category, String categoryLable, Float price, Long commnetNum, String author, String publish) {
        this.source = source;
        this.bookName = bookName;
        this.category = category;
        this.categoryLable = categoryLable;
        this.price = price;
        this.commnetNum = commnetNum;
        this.author = author;
        this.publish = publish;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCategoryLable() {
        return categoryLable;
    }

    public void setCategoryLable(String categoryLable) {
        this.categoryLable = categoryLable;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float price) {
        this.price = price;
    }

    public Long getCommnetNum() {
        return commnetNum;
    }

    public void setCommnetNum(Long commnetNum) {
        this.commnetNum = commnetNum;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getPublish() {
        return publish;
    }

    public void setPublish(String publish) {
        this.publish = publish;
    }
}
