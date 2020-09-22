package org.apache.storm.multicast.dynamicswitch;

/**
 * locate org.apache.storm.multicast.dynamicswitch
 * Created by master on 2019/10/31.
 * 控制配置信息，用于通知下游目标实例与相应的实例建立连接或者断开连接
 */
public class ControlConfiguration {
    private String disconnect = null;
    private String reconnect = null;
    private Integer id = null;
    private Integer layer = null;
    private Integer index = null;

    @Override
    public String toString() {
        return (disconnect == null ? "null" : disconnect) + " " + (reconnect == null ? "null" : reconnect) + " " + (id == null ? "null" : id) + " " + (layer == null ? "null" : layer) + " " + (index == null ? "null" : index);
    }

    public boolean shouldUpdate() {
        return shouldReconnect() || shouldMetaUpdate();
    }

    public boolean shouldReconnect() {
        return !(disconnect == null && reconnect == null);
    }

    public boolean shouldMetaUpdate() {
        return id != null || layer != null || index != null;
    }

    public ControlConfiguration setDisconnect(String disconnect) {
        this.disconnect = disconnect;
        if (this.disconnect.equals(this.reconnect)) {
            this.disconnect = null;
            this.reconnect = null;
        }
        return this;
    }

    public ControlConfiguration setReconnect(String reconnect) {
        this.reconnect = reconnect;
        if (this.reconnect.equals(this.disconnect)) {
            this.reconnect = null;
            this.disconnect = null;
        }
        return this;
    }

    public ControlConfiguration setId(Integer id) {
        this.id = id;
        return this;
    }

    public ControlConfiguration setIndex(Integer index) {
        this.index = index;
        return this;
    }

    public ControlConfiguration setLayer(Integer layer) {
        this.layer = layer;
        return this;
    }

    public ControlConfiguration setMeta(Integer id, Integer layer, Integer index) {
        return setId(id).setLayer(layer).setIndex(index);
    }

    public String getDisconnect() {
        return disconnect;
    }

    public String getReconnect() {
        return reconnect;
    }

    public Integer getId() {
        return id;
    }

    public Integer getLayer() {
        return layer;
    }

    public Integer getIndex() {
        return index;
    }
}
