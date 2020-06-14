package com.hadoop.demo.zookeeper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author yangwj
 * @date 2020/6/12 11:57
 */
public class test2 {


    public static void main(String[] args) throws Exception {
        Set<String> s = new HashSet<>();
        s.add("1");
        s.add("2");
        Object[] objects = s.toArray();
        String str = Arrays.toString(objects);
        System.out.println(Arrays.toString(objects));
        str = str.substring(1, str.length()-1);
        System.out.println(str);

    }




}
