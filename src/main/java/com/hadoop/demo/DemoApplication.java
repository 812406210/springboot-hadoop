package com.hadoop.demo;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@MapperScan("com.hadoop.demo.dao")
@SpringBootApplication
//@ComponentScan(value = "com.hadoop.demo.config")
public class DemoApplication {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-dev\\hadoop-2.7.7");
        SpringApplication.run(DemoApplication.class, args);
    }

}
