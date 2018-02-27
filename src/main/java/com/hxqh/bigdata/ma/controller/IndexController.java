package com.hxqh.bigdata.ma.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;


/**
 * @author Ocean Lin
 * <p>
 * Created by Ocean lin on 2018-2-27.
 */
@Controller
public class IndexController {


    @RequestMapping("/")
    public String index() {

        return "index/index";
    }


}


