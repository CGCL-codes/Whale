package org.apache.storm.benchmark.model;

import java.io.Serializable;

/**
 * locate org.apache.storm.benchmark.didimatch.model
 * Created by master on 2019/10/15.
 */
public class DiDiOrder implements Serializable {
    private String orderId;
    private String time;
    private String matchDriverId;

    public DiDiOrder() {
    }

    public DiDiOrder(String orderId, String time, String matchDriverId) {
        this.orderId = orderId;
        this.time = time;
        this.matchDriverId = matchDriverId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getMatchDriverId() {
        return matchDriverId;
    }

    public void setMatchDriverId(String matchDriverId) {
        this.matchDriverId = matchDriverId;
    }

    @Override
    public String toString() {
        return "Orders{" +
                "orderId='" + orderId + '\'' +
                ", time='" + time + '\'' +
                ", matchDriverId='" + matchDriverId + '\'' +
                '}';
    }
}
