package org.apache.storm.messaging;

/**
 * locate org.apache.storm.messaging
 * Created by MasterTj on 2019/10/12.
 * CGCL RDMA Cluster Configuration
 */
public class ClusterAddressHost {

    /**
     *  网卡地址转换(TCP 转换 RDMA)
     * @param host 传统网络IP地址
     * @return
     */
    public static String resovelAddressHost(String host){
        return "i"+host;
    }
}
