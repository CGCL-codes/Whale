package org.apache.storm.util;

import org.junit.Test;

/**
 * locate org.apache.storm.util
 * Created by master on 2019/10/9.
 */
public class PropertiesUtilTest {
    static {
        PropertiesUtil.init("/whale.properties");
    }
    @Test
    public void getProperties() {
        System.out.println(PropertiesUtil.getProperties("KAFKA_LOCAL_BROKER"));
        System.out.println(PropertiesUtil.getProperties("C3P0_DATABASE_CONFIG"));
    }
}
