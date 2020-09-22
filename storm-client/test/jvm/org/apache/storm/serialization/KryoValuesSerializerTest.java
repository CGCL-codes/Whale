package org.apache.storm.serialization;

import org.apache.storm.utils.Utils;
import org.junit.Test;

/**
 * locate org.apache.storm.serialization
 * Created by tjmaster on 18-2-2.
 */
public class KryoValuesSerializerTest {

    @Test
    public void testSerializer(){
        KryoValuesSerializer kryoValuesSerializer=new KryoValuesSerializer(Utils.readDefaultConfig());
        kryoValuesSerializer.serializeObject(new Integer(1));
    }
}
