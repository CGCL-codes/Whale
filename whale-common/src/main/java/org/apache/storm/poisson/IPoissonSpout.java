package org.apache.storm.poisson;

import org.apache.storm.topology.IRichSpout;

public interface IPoissonSpout extends IRichSpout {
    /**
     * nextTuple会调用next多次以实现吞吐量控制
     */
    void next();
}
