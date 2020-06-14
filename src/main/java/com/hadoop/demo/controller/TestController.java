package com.hadoop.demo.controller;

import com.hadoop.demo.dao.UserDao;
import com.hadoop.demo.entity.User;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;
import java.util.Optional;

@RestController
public class TestController {
    @Resource
    UserDao userDao;

    @RequestMapping("test1")
    public String test1(){
        return "success";
    }

    @RequestMapping("testUser")
    public User testUser(){

        User user = userDao.selectById(1);
        return user;
    }

}
