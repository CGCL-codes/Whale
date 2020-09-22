package org.apache.storm.multicast.util;

/**
 * locate org.apache.storm.multicast.util
 * Created by master on 2020/5/25.
 */
public class ModelConstants {
    //negative scale-down 动态切换策略
    public static final String NegativeSacleDown = "NegativeSacle-down";
    //active scale-up 动态切换策略
    public static final String ActiveSacleUp = "ActiveSacle-up";

    //tuple 平均处理延迟
    public static final double tupleProcessTime = 0.0052;
    //传输队列容量
    public static final int queueCapacity = 1024;
    //警戒值水位线
    public static final int warningWaterLine = (int) (1024*0.9);
    //negative scale-down 动态切换策略的阈值 (减少源头实例的出度)
    public static final double threshold_down = 0.8;
    //active scale-up 动态切换策略的阈值 (增加源头实例的出度)
    public static final double threshold_up = 0.6;
}
