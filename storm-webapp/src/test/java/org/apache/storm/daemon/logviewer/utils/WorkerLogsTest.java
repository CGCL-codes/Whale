/*
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

package org.apache.storm.daemon.logviewer.utils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.storm.daemon.logviewer.testsupport.MockDirectoryBuilder;
import org.apache.storm.daemon.logviewer.testsupport.MockFileBuilder;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.utils.Utils;
import org.junit.Test;

public class WorkerLogsTest {

    /**
     * Build up workerid-workerlogdir map for the old workers' dirs.
     */
    @Test
    public void testIdentifyWorkerLogDirs() throws Exception {
        File port1Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo1/port1").build();
        File mockMetaFile = new MockFileBuilder().setFileName("worker.yaml").build();

        String expId = "id12345";
        Map<String, File> expected = Collections.singletonMap(expId, port1Dir);

        try {
            SupervisorUtils mockedSupervisorUtils = mock(SupervisorUtils.class);
            SupervisorUtils.setInstance(mockedSupervisorUtils);

            Map<String, Object> stormConf = Utils.readStormConfig();
            WorkerLogs workerLogs = new WorkerLogs(stormConf, port1Dir) {
                @Override
                public Optional<File> getMetadataFileForWorkerLogDir(File logDir) throws IOException {
                    return Optional.of(mockMetaFile);
                }

                @Override
                public String getWorkerIdFromMetadataFile(String metaFile) {
                    return expId;
                }
            };

            when(mockedSupervisorUtils.readWorkerHeartbeatsImpl(anyMapOf(String.class, Object.class))).thenReturn(null);
            assertEquals(expected, workerLogs.identifyWorkerLogDirs(Collections.singleton(port1Dir)));
        } finally {
            SupervisorUtils.resetInstance();
        }
    }

}
