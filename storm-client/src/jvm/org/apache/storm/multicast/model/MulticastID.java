package org.apache.storm.multicast.model;

import java.io.Serializable;

/**
 * locate org.apache.storm.multicast.core
 * Created by MasterTj on 2019/10/9.
 */
public class MulticastID implements Serializable {

    private int id;
    private int layer;
    private int index;

    public MulticastID(String componentID){
        String[] split = componentID.split("-");
        this.id = Integer.valueOf(split[1]);
        this.layer = Integer.valueOf(split[2]);
        this.index = Integer.valueOf(split[3]);
    }

    public MulticastID(int layer, int id, int index) {
        this.layer = layer;
        this.id = id;
        this.index = index;
    }

    public int getLayer() {
        return layer;
    }

    public void setLayer(int layer) {
        this.layer = layer;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "MulticastID{" +
                "id=" + id +
                ", layer=" + layer +
                ", index=" + index +
                '}';
    }
}
