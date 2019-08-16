package com.hadoop.demo.service;


import com.hadoop.demo.entity.BusReceiverEntity;
import java.io.Serializable;
import java.util.List;

public interface BusReceiverService {

    void save(String tableName,BusReceiverEntity busReceiverEntity);
    void batchSave(List<BusReceiverEntity> list);
    BusReceiverEntity queryByRowId(Serializable id);
    void deleteAll(String table);
    void createTable(String tableName,String[] familys);
}
