/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.messaging.rdmabench;

import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.messaging.netty.ControlMessage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

////////////////////////////////////优化transferAllGrouping/////////////////////////
public class MessageDecoder {
    /*
     * Each ControlMessage is encoded as:
     *  code (<0) ... short(2)
     * Each WorkerMessage is encoded as:
     *  tasks_size short(2)
     *  tasks_id ... List<short>(2)
     *  len ... int(4)
     *  payload ... byte[]    *
     */
    protected Object decode( ByteBuffer buf) throws Exception {
        // Make sure that we have received at least a short
        buf.clear();
        int available = buf.getInt();

        List<Object> ret = new ArrayList<>();
        // Use while loop, try to decode as more messages as possible in single call

        while (available >= 2) {

            // read the short field
            short code = buf.getShort();
            available -= 2;

            // case 1: Control message
            ControlMessage ctrl_msg = ControlMessage.mkMessage(code);
            if (ctrl_msg != null) {

                if (ctrl_msg == ControlMessage.EOB_MESSAGE) {
                    break;
                } else {
                    return ctrl_msg;
                }
            }

            // case : Worker Message

            // Read the tasks_id field
            List<Integer> task_ids=new ArrayList<>();
            for(int i=0;i<code;i++){
                task_ids.add((int)buf.getShort());
            }

            // Read the length field.
            int length = buf.getInt();

            available -= (4+2*code);

            if (length <= 0) {
                //ret.add(new WorkerMessage(task_ids, null));
                System.out.println("length: "+ length);
                continue;
            }

            available -= length;

            // There's enough bytes in the buffer. Read it.
            byte[] payload =new byte[length];
            buf.get(payload);

            // Successfully decoded a frame.
            // Return a TaskMessage object
            ret.add(new WorkerMessage(task_ids, payload));
        }
        if(available!=0)
            System.out.println(available);
        if (ret.size() == 0) {
            return null;
        } else {
            return ret;
        }
    }
}
