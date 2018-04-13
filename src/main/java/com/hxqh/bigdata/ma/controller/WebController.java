package com.hxqh.bigdata.ma.controller;

import com.hxqh.bigdata.ma.common.Constants;
import com.hxqh.bigdata.ma.model.base.Message;
import com.hxqh.bigdata.ma.service.MarketInteractiveService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * Created by Ocean lin on 2018/4/8.
 *
 * @author Ocean lin
 */
@RestController
@RequestMapping("/web")
public class WebController {
    private Logger logger = Logger.getLogger(WebController.class);

    @Autowired
    private MarketInteractiveService marketInteractiveService;

    @RequestMapping("/execute")
    @ResponseBody
    public Message execute() {

        logger.info("start submit spark task...");
        Message message = null;
        try {
            marketInteractiveService.run();
            message = new Message(Constants.SUCCESS, Constants.ADDSUCCESS);
        } catch (Exception e) {
            message = new Message(Constants.FAIL, Constants.ADDFAIL);
            e.printStackTrace();
        } finally {
            return message;
        }
    }


}
