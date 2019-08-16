package com.hadoop.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hadoop.demo.entity.BusReceiverEntity;
import com.hadoop.demo.service.BusReceiverService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.ArrayList;
import java.util.List;

@RestController
public class HbaseController {

    @Autowired
    private BusReceiverService busReceiverService;

    @RequestMapping("/createTable")
    public String createTable(@RequestParam(value = "tableName") String tableName){
        String[]  family= {"base","extends"};
        busReceiverService.createTable(tableName,family);
        return "createTable";
    }

    @RequestMapping("/save")
    public String save(@RequestParam String tableName){
        BusReceiverEntity busReceiverEntity = new BusReceiverEntity();
        busReceiverEntity.setId(1);
        busReceiverEntity.setAddress("address001");
        busReceiverEntity.setEnName("enName001");
        busReceiverEntity.setMemberFamily("MemberFamily001");
        busReceiverEntity.setRegionCode("regionCode001");
        busReceiverEntity.setName("name001");
        busReceiverService.save(tableName,busReceiverEntity);
        return "save";
    }

    @RequestMapping("/batchSave/{count}")
    public String batchSave(@PathVariable int count){
        List<BusReceiverEntity> sList = new ArrayList<BusReceiverEntity>();
        for (int i = 0; i < count; i++) {
            sList.add(sList.get(i));
            if(sList.size()%20000==0
                    || i==count-1 ){
                busReceiverService.batchSave(sList);
                sList.clear();
            }

        }
        return "saveBatch";
    }

    @RequestMapping("/findById/{id}")
    public String findById(@PathVariable String id){
        return JSON.toJSONString(busReceiverService.queryByRowId(id));
    }

    @RequestMapping("/deleteAll")
    public String deleteAll(@RequestParam String tableName){
        busReceiverService.deleteAll(tableName);
        return "deleteAll";
    }


}