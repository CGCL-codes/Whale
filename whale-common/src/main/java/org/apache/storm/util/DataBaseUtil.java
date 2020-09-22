package org.apache.storm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;


/**
 * Created by 79875 on 2017/1/9.
 */
public class DataBaseUtil {

    private static Logger logger= LoggerFactory.getLogger(DataBaseUtil.class);

    public static Connection conn;//创建用于连接数据库的Connection对象

    /**
     * 插入数据到t_latency数据库中
     * @param offset
     * @param maxdelay
     * @param mindelay
     * @param middledelay
     * @param timeinfo
     */
    public static void insertDiDiDelay(long offset , long maxdelay, long mindelay, long middledelay, Timestamp timeinfo){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_delay(offset,maxdelay,mindelay,middledelay,time)"
                    + " VALUES (?,?,?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setLong(1,offset);
            preparedStatement.setLong(2,maxdelay);
            preparedStatement.setLong(3,mindelay);
            preparedStatement.setLong(4,middledelay);
            preparedStatement.setTimestamp(5,timeinfo);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据到tupleCount表中
     * @param taskId
     * @param latency
     * @param timeinfo
     */
    public static void insertDiDiLatency(int taskId , double latency, Timestamp timeinfo){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_latency(taskid,latency,time)"
                    + " VALUES (?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setInt(1,taskId);
            preparedStatement.setDouble(2,latency);
            preparedStatement.setTimestamp(3,timeinfo);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据到t_throughput表中
     * @param taskId
     * @param throughput
     * @param timeinfo
     */
    public static void insertDiDiThroughput(int taskId , long throughput, Timestamp timeinfo){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_throughput(taskid,throughput,time)"
                    + " VALUES (?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setInt(1,taskId);
            preparedStatement.setLong(2,throughput);
            preparedStatement.setTimestamp(3,timeinfo);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
