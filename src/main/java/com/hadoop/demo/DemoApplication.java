package com.hadoop.demo;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@MapperScan("com.hadoop.demo.dao")
@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-dev\\hadoop-2.7.7");
        SpringApplication.run(DemoApplication.class, args);
    }

}
