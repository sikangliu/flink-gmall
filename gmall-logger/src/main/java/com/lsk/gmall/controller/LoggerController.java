package com.lsk.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2022-07-26 17:08
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test1")
    public String test1() {
        System.out.println("11111111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name,
                        @RequestParam("age") int age) {
        System.out.println(name + ":" + age);
        return "success";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        //落盘
        log.info(jsonStr);
        //写入 Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }
}