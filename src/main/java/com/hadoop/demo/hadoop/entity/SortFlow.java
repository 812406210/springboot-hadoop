package com.hadoop.demo.hadoop.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/5 15:58
 * @desc 流量
 */
public class SortFlow implements WritableComparable<SortFlow> {
    // 上行流量
    private Long upStream;
    //下行流量
    private Long downStream;
    //总流量
    private Long sumStream;

    public SortFlow() { }

    public Long getUpStream() {
        return upStream;
    }

    public void setUpStream(Long upStream) {
        this.upStream = upStream;
    }

    public Long getDownStream() {
        return downStream;
    }

    public void setDownStream(Long downStream) {
        this.downStream = downStream;
    }

    public Long getSumStream() {
        return sumStream;
    }

    public void setSumStream(Long sumStream) {
        this.sumStream = sumStream;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upStream);
        dataOutput.writeLong(downStream);
        dataOutput.writeLong(sumStream);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upStream = dataInput.readLong();
        this.downStream = dataInput.readLong();
        this.sumStream = dataInput.readLong();
    }

    @Override
    public int compareTo(SortFlow o) {
        return Long.compare(o.sumStream,this.sumStream);
    }
}
