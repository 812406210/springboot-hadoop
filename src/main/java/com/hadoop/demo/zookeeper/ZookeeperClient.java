package com.hadoop.demo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author yangwj
 * @date 2020/6/7 22:33
 */
public class ZookeeperClient {
    private static ZooKeeper zk = null;

    public static void init(){
        try {
            zk = new ZooKeeper("localhost:2181", 2000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    try {
                        //设置驻留进行监听节点变化
                        zk.getData("/yang",true,null);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("发生变化啦");
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) throws Exception {
//        testCreate();
//        testUpdate();
//        testDelete();
//        testGet();
//        init();
//        testGet();
//        Thread.sleep(600000);

       // plusDay(3);

    }


    public static void testCreate() throws Exception {
        String path = zk.create("/yang","hello yang".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.PERSISTENT);
        System.out.println(path);
    }

    public static void testUpdate() throws Exception {
        Stat path = zk.setData("/yang","hello yang---update".getBytes(),-1);
        System.out.println(path);
    }

    public static void testDelete() throws Exception {
        zk.delete("/yang",-1);
    }

    public static void testGet() throws Exception {
        byte[] data = zk.getData("/yang", true, null);
        System.out.println(new String(data));
    }

    public static void testGetChild() throws Exception {
        List<String> data = zk.getChildren("/yang",true);
       System.out.println("data--->"+data);
        for (String childPath : data) {
            byte[] d = zk.getData("/yang/"+childPath,true,null);
            System.out.println(new String(d));
        }

    }





}
