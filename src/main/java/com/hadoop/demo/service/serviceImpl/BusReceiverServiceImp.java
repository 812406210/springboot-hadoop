package com.hadoop.demo.service.serviceImpl;

import com.hadoop.demo.entity.BusReceiverEntity;
import com.hadoop.demo.service.BusReceiverService;
import com.hadoop.demo.utils.HBaseColumn;
import com.hadoop.demo.utils.HQuery;
import com.hadoop.demo.utils.HbaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.Serializable;
import java.util.List;

@Service
@Slf4j
public class BusReceiverServiceImp implements BusReceiverService {

    @Autowired
    private HbaseUtil hbaseUtil;

    /**
     * 创建表
     */
    public  void createTable(String tableName,String[] familys){

       // String[] familys = {"base","extends"};
        //TableName tableName = TableName.valueOf("bus_receiver");
        hbaseUtil.createTable(tableName,familys);
    }

    @Override
    public void save(String tableName,BusReceiverEntity busReceiverEntity) {
        HQuery hQuery = new HQuery(tableName,String.valueOf(busReceiverEntity.getId()));
        hQuery.getColumns().add(new HBaseColumn("base","name",busReceiverEntity.getName(),busReceiverEntity.getId()));
        hQuery.getColumns().add(new HBaseColumn("base","regionCode",busReceiverEntity.getRegionCode(),busReceiverEntity.getId()));
        hQuery.getColumns().add(new HBaseColumn("extends","address",busReceiverEntity.getAddress(),busReceiverEntity.getId()));
        hQuery.getColumns().add(new HBaseColumn("extends","memberFamily",String.valueOf(busReceiverEntity.getMemberFamily()),busReceiverEntity.getId()));
        hQuery.getColumns().add(new HBaseColumn("extends","enName",String.valueOf(busReceiverEntity.getEnName()),busReceiverEntity.getId()));
        hbaseUtil.bufferInsert(hQuery);
    }

    public void batchSave(List<BusReceiverEntity> list){
        HQuery hQuery = new HQuery("nias:bus_receiver");
        for(BusReceiverEntity busReceiverEntity:list){
            hQuery.getColumns().add(new HBaseColumn("base","name",busReceiverEntity.getName(),busReceiverEntity.getId()));
            hQuery.getColumns().add(new HBaseColumn("base","regionCode",busReceiverEntity.getRegionCode(),busReceiverEntity.getId()));
            hQuery.getColumns().add(new HBaseColumn("extends","address",busReceiverEntity.getAddress(),busReceiverEntity.getId()));
            hQuery.getColumns().add(new HBaseColumn("extends","memberFamily",String.valueOf(busReceiverEntity.getMemberFamily()),busReceiverEntity.getId()));
            hQuery.getColumns().add(new HBaseColumn("extends","enName",String.valueOf(busReceiverEntity.getEnName()),busReceiverEntity.getId()));
        }
        hbaseUtil.bufferInsert(hQuery);
    }

    @Override
    public BusReceiverEntity queryByRowId(Serializable id) {
        HQuery hQuery = new HQuery("nias:bus_receiver",id);
        BusReceiverEntity busReceiverEntity = hbaseUtil.get(hQuery,BusReceiverEntity.class);
        return busReceiverEntity;
    }

    /**
     * 删除表
     * @param table
     */
    public void deleteAll(String table){
        hbaseUtil.deleteAll(table);
    }


}
