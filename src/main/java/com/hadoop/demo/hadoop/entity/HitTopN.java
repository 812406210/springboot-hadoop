package com.hadoop.demo.hadoop.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/5 23:03
 */
public class HitTopN implements Writable {

    private int times;
    private String url;

    public HitTopN() { }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getTimes() {
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
    }

    @Override
    public String toString() {
        return "HitTopN{" +
                "times=" + times +
                ", url=" + url +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(times);
            dataOutput.writeUTF(url);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            this.times = dataInput.readInt();
            this.url = dataInput.readUTF();
    }
}
