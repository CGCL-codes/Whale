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

package org.apache.storm.daemon.logviewer.handler;

import java.io.IOException;
import javax.ws.rs.core.Response;

import org.apache.storm.daemon.logviewer.utils.LogFileDownloader;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;

public class LogviewerLogDownloadHandler {

    private WorkerLogs workerLogs;
    private final LogFileDownloader logFileDownloadHelper;

    /**
     * Constructor.
     *
     * @param logRoot root worker log directory
     * @param daemonLogRoot root daemon log directory
     * @param workerLogs {@link WorkerLogs}
     * @param resourceAuthorizer {@link ResourceAuthorizer}
     */
    public LogviewerLogDownloadHandler(String logRoot, String daemonLogRoot, WorkerLogs workerLogs, ResourceAuthorizer resourceAuthorizer) {
        this.workerLogs = workerLogs;
        this.logFileDownloadHelper = new LogFileDownloader(logRoot, daemonLogRoot, resourceAuthorizer);
    }

    /**
     * Download an worker log.
     *
     * @param fileName file to download
     * @param user username
     * @return a Response which lets browsers download that file.
     * @see {@link LogFileDownloader#downloadFile(String, String, boolean)}
     */
    public Response downloadLogFile(String fileName, String user) throws IOException {
        workerLogs.setLogFilePermission(fileName);
        return logFileDownloadHelper.downloadFile(fileName, user, false);
    }

    /**
     * Download a daemon log.
     *
     * @param fileName file to download
     * @param user username
     * @return a Response which lets browsers download that file.
     * @see {@link LogFileDownloader#downloadFile(String, String, boolean)}
     */
    public Response downloadDaemonLogFile(String fileName, String user) throws IOException {
        return logFileDownloadHelper.downloadFile(fileName, user, true);
    }

}
