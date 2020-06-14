package com.hadoop.demo.zookeeper;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author yangwj
 * @date 2020/6/9 18:13
 */
public class test {
    public static void main(String[] args) throws ParseException {




        Date dates = new Date();//当前时间
        int workDay = 2;// 工作日天数
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 格式化时间
        String source = df.format(dates);// 格式化开始时间:2019-11-1 10:45:06
        //Date类型：df.parse(source)
//        System.out.println(df.format(getWorkDay(df.parse(source), workDay)));
    }

    public static Date plusDay(int num) throws ParseException {
        Date date = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date currdate = format.parse("2019-02-15 18:46:31");
                //(format.format(date));

        System.out.println("现在的日期是：" + currdate);
        Calendar ca = Calendar.getInstance();
        ca.setTime(currdate);
        ca.add(Calendar.DATE, num);// num为增加的天数，可以改变的
        currdate = ca.getTime();
        String enddate = format.format(currdate);
        System.out.println("增加天数以后的日期：" + currdate);
        System.out.println("增加天数以后的日期：" + enddate);
        return currdate;
    }


}
