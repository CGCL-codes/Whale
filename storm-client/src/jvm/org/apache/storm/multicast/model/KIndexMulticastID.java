package org.apache.storm.multicast.model;

import java.io.Serializable;

/**
 * locate org.apache.storm.multicast.core
 * Created by MasterTj on 2019/10/9.
 */
public class KIndexMulticastID implements Serializable {
    private int KIndex;
    private int id;
    private int layer;
    private int index;

    public KIndexMulticastID(String componentID){
        String[] split = componentID.split("-");
        this.KIndex = Integer.valueOf(split[0]);
        this.id = Integer.valueOf(split[2]);
        this.layer = Integer.valueOf(split[3]);
        this.index = Integer.valueOf(split[4]);
    }

    public KIndexMulticastID(int KIndex, int layer, int id, int index) {
        this.KIndex = KIndex;
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

    public int getKIndex() {
        return KIndex;
    }

    public void setKIndex(int KIndex) {
        this.KIndex = KIndex;
    }

    @Override
    public String toString() {
        return "KIndexMulticastID{" +
                "KIndex=" + KIndex +
                ", id=" + id +
                ", layer=" + layer +
                ", index=" + index +
                '}';
    }
}
