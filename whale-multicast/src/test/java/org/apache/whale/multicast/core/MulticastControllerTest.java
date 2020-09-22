package org.apache.whale.multicast.core;

import org.junit.Test;

/**
 * locate org.apache.storm.multicast.core
 * Created by master on 2020/5/25.
 */
public class MulticastControllerTest {

    @Test
    public void computeMaxOutDegree() {
        int i = MulticastController.computeMaxOutDegree(100000, -1);
        System.out.println(i);
    }
}
