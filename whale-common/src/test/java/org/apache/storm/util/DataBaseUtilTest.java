package org.apache.storm.util;

import org.junit.Test;

import java.sql.Timestamp;

/**
 * locate org.apache.storm.util
 * Created by master on 2019/10/10.
 */
public class DataBaseUtilTest {

    @Test
    public void insertDiDiDelay() {
    }

    @Test
    public void insertDiDiLatency() {
        DataBaseUtil.insertDiDiLatency(1,1000,new Timestamp(System.currentTimeMillis()));
    }


    @Test
    public void insertDiDiThroughput() {
        DataBaseUtil.insertDiDiThroughput(1,1000,new Timestamp(System.currentTimeMillis()));
    }
}
