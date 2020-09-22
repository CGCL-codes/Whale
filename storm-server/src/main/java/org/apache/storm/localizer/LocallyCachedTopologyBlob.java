/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.localizer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A locally cached blob for the topology.  storm.jar, stormcode.ser, or stormconf.ser.
 * The version number of the blob's file will be stored in `${basename}.version`
 */
public class LocallyCachedTopologyBlob extends LocallyCachedBlob {
    private static final Logger LOG = LoggerFactory.getLogger(LocallyCachedTopologyBlob.class);
    public static final long LOCAL_MODE_JAR_VERSION = 1;

    private static String resourcesJar() throws IOException {
        String path = ServerUtils.currentClasspath();
        if (path == null) {
            return null;
        }

        for (String jpath : path.split(File.pathSeparator)) {
            if (jpath.endsWith(".jar")) {
                if (ServerUtils.zipDoesContainDir(jpath, ServerConfigUtils.RESOURCES_SUBDIR)) {
                    return jpath;
                }
            }
        }
        return null;
    }

    public enum TopologyBlobType {
        TOPO_JAR("stormjar.jar", "-stormjar.jar", "resources"),
        TOPO_CODE("stormcode.ser", "-stormcode.ser", null),
        TOPO_CONF("stormconf.ser", "-stormconf.ser", null);

        private final String fileName;
        private final String keySuffix;
        private final String extractionDir;

        TopologyBlobType(String fileName, String keySuffix, String extractionDir) {
            this.fileName = fileName;
            this.keySuffix = keySuffix;
            this.extractionDir = extractionDir;
        }

        public String getFileName() {
            return fileName;
        }

        public String getTempFileName(long version) {
            return fileName + "." + version;
        }

        public String getVersionFileName() {
            return fileName + ".version";
        }

        public String getKey(String topologyId) {
            return topologyId + keySuffix;
        }

        public boolean needsExtraction() {
            return extractionDir != null;
        }

        public String getExtractionDir() {
            return extractionDir;
        }

        public String getTempExtractionDir(long version) {
            return extractionDir + "." + version;
        }
    }

    private final TopologyBlobType type;
    private final String topologyId;
    private final boolean isLocalMode;
    private final Path topologyBasicBlobsRootDir;
    private final AdvancedFSOps fsOps;
    private volatile long version = NOT_DOWNLOADED_VERSION;
    private volatile long size = 0;

    /**
     * Create a new LocallyCachedBlob.
     *
     * @param topologyId the ID of the topology.
     * @param type the type of the blob.
     */
    protected LocallyCachedTopologyBlob(final String topologyId, final boolean isLocalMode, final Map<String, Object> conf,
                                        final AdvancedFSOps fsOps, final TopologyBlobType type) throws IOException {
        super(topologyId + " " + type.getFileName(), type.getKey(topologyId));
        this.topologyId = topologyId;
        this.type = type;
        this.isLocalMode = isLocalMode;
        this.fsOps = fsOps;
        topologyBasicBlobsRootDir = Paths.get(ConfigUtils.supervisorStormDistRoot(conf, topologyId));
        readVersion();
        updateSizeOnDisk();
    }

    private void updateSizeOnDisk() throws IOException {
        long total = getSizeOnDisk(topologyBasicBlobsRootDir.resolve(type.getFileName()));
        if (type.needsExtraction()) {
            total += getSizeOnDisk(topologyBasicBlobsRootDir.resolve(type.getExtractionDir()));
        }
        size = total;
    }

    private void readVersion() throws IOException {
        Path versionFile = topologyBasicBlobsRootDir.resolve(type.getVersionFileName());
        if (!fsOps.fileExists(versionFile)) {
            version = NOT_DOWNLOADED_VERSION;
        } else {
            String ver = FileUtils.readFileToString(versionFile.toFile(), "UTF8").trim();
            version = Long.parseLong(ver);
        }
    }

    @Override
    public long getLocalVersion() {
        LOG.debug("LOCAL VERSION {}/{} is {}", type, topologyId, version);
        return version;
    }

    @Override
    public long getRemoteVersion(ClientBlobStore store) throws KeyNotFoundException, AuthorizationException {
        if (isLocalMode && type == TopologyBlobType.TOPO_JAR) {
            LOG.debug("REMOTE VERSION LOCAL JAR {}", LOCAL_MODE_JAR_VERSION);
            return LOCAL_MODE_JAR_VERSION;
        }
        return store.getBlobMeta(type.getKey(topologyId)).get_version();
    }

    @Override
    public long downloadToTempLocation(ClientBlobStore store)
        throws IOException, KeyNotFoundException, AuthorizationException {
        if (isLocalMode && type == TopologyBlobType.TOPO_JAR) {
            LOG.debug("DOWNLOADING LOCAL JAR to TEMP LOCATION... {}", topologyId);
            //This is a special case where the jar was not uploaded so we will not download it (it is already on the classpath)
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            String resourcesJar = resourcesJar();
            URL url = classloader.getResource(ServerConfigUtils.RESOURCES_SUBDIR);
            Path extractionDest = topologyBasicBlobsRootDir.resolve(type.getTempExtractionDir(LOCAL_MODE_JAR_VERSION));
            if (resourcesJar != null) {
                LOG.info("Extracting resources from jar at {} to {}", resourcesJar, extractionDest);
                extractDirFromJar(resourcesJar, ServerConfigUtils.RESOURCES_SUBDIR, extractionDest);
            } else if (url != null) {
                LOG.info("Copying resources at {} to {}", url, extractionDest);
                if ("jar".equals(url.getProtocol())) {
                    JarURLConnection urlConnection = (JarURLConnection) url.openConnection();
                    extractDirFromJar(urlConnection.getJarFileURL().getFile(), ServerConfigUtils.RESOURCES_SUBDIR, extractionDest);
                } else {
                    fsOps.copyDirectory(new File(url.getFile()), extractionDest.toFile());
                }
            }
            return LOCAL_MODE_JAR_VERSION;
        }


        long newVersion = downloadToTempLocation(store, type.getKey(topologyId), version, fsOps,
            (version) -> topologyBasicBlobsRootDir.resolve(type.getTempFileName(version)));

        Path tmpLocation = topologyBasicBlobsRootDir.resolve(type.getTempFileName(newVersion));

        if (type.needsExtraction()) {
            Path extractionDest = topologyBasicBlobsRootDir.resolve(type.getTempExtractionDir(newVersion));
            extractDirFromJar(tmpLocation.toAbsolutePath().toString(), ServerConfigUtils.RESOURCES_SUBDIR,
                extractionDest);
        }
        return newVersion;
    }

    protected void extractDirFromJar(String jarpath, String dir, Path dest) throws IOException {
        LOG.debug("EXTRACTING {} from {} and placing it at {}", dir, jarpath, dest);
        if (!Files.exists(dest)) {
            //Create the directory no matter what. This is so we can check if it was downloaded in the future.
            Files.createDirectories(dest);
        }
        try (JarFile jarFile = new JarFile(jarpath)) {
            String toRemove = dir + '/';
            Enumeration<JarEntry> jarEnums = jarFile.entries();
            while (jarEnums.hasMoreElements()) {
                JarEntry entry = jarEnums.nextElement();
                String name = entry.getName();
                if (!entry.isDirectory() && name.startsWith(toRemove)) {
                    String shortenedName = name.replace(toRemove, "");
                    Path targetFile = dest.resolve(shortenedName);
                    LOG.debug("EXTRACTING {} SHORTENED to {} into {}", name, shortenedName, targetFile);
                    fsOps.forceMkdir(targetFile.getParent());
                    try (FileOutputStream out = new FileOutputStream(targetFile.toFile());
                         InputStream in = jarFile.getInputStream(entry)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        }
    }

    @Override
    public boolean isFullyDownloaded() {
        Path versionFile = topologyBasicBlobsRootDir.resolve(type.getVersionFileName());
        boolean ret = Files.exists(versionFile);
        Path dest = topologyBasicBlobsRootDir.resolve(type.getFileName());
        if (!(isLocalMode && type == TopologyBlobType.TOPO_JAR)) {
            ret = ret && Files.exists(dest);
        }
        if (type.needsExtraction()) {
            Path extractionDest = topologyBasicBlobsRootDir.resolve(type.getExtractionDir());
            ret = ret && Files.exists(extractionDest);
        }
        return ret;
    }

    @Override
    public void commitNewVersion(long newVersion) throws IOException {
        //This is not atomic (so if something bad happens in the middle we need to be able to recover
        Path tempLoc = topologyBasicBlobsRootDir.resolve(type.getTempFileName(newVersion));
        Path dest = topologyBasicBlobsRootDir.resolve(type.getFileName());
        Path versionFile = topologyBasicBlobsRootDir.resolve(type.getVersionFileName());

        LOG.debug("Removing version file {} to force download on failure", versionFile);
        fsOps.deleteIfExists(versionFile.toFile()); //So if we fail we are forced to try again
        LOG.debug("Removing destination file {} in preparation for move", dest);
        fsOps.deleteIfExists(dest.toFile());
        if (type.needsExtraction()) {
            Path extractionTemp = topologyBasicBlobsRootDir.resolve(type.getTempExtractionDir(newVersion));
            Path extractionDest = topologyBasicBlobsRootDir.resolve(type.getExtractionDir());
            LOG.debug("Removing extraction dest {} in preparation for extraction", extractionDest);
            fsOps.deleteIfExists(extractionDest.toFile());
            if (fsOps.fileExists(extractionTemp)) {
                fsOps.moveDirectoryPreferAtomic(extractionTemp.toFile(), extractionDest.toFile());
            }
        }
        if (!(isLocalMode && type == TopologyBlobType.TOPO_JAR)) {
            //Don't try to move the JAR file in local mode, it does not exist because it was not uploaded
            Files.move(tempLoc, dest);
        }
        LOG.debug("Writing out version file {} with version {}", versionFile, newVersion);
        FileUtils.write(versionFile.toFile(), Long.toString(newVersion), "UTF8");
        this.version = newVersion;
        updateSizeOnDisk();
        LOG.debug("New version of {} - {} committed {}", topologyId, type, newVersion);
    }

    @Override
    public void cleanupOrphanedData() throws IOException {
        cleanUpTemp(type.getFileName());
        if (type.needsExtraction()) {
            cleanUpTemp(type.getExtractionDir());
        }
    }

    private static final Pattern EXTRACT_BASE_NAME_AND_VERSION = Pattern.compile("^(.*)\\.([0-9]+)$");

    private void cleanUpTemp(String baseName) throws IOException {
        LOG.debug("Cleaning up temporary data in {}", topologyBasicBlobsRootDir);
        try (DirectoryStream<Path> children = fsOps.newDirectoryStream(topologyBasicBlobsRootDir,
            (p) -> {
                String fileName = p.getFileName().toString();
                Matcher m = EXTRACT_BASE_NAME_AND_VERSION.matcher(fileName);
                return m.matches() && baseName.equals(m.group(1));
            })) {
            //children is only ever null if topologyBasicBlobsRootDir does not exist.  This happens during unit tests
            // And because a non-existant directory is by definition clean we are ignoring it.
            if (children != null) {
                for (Path p : children) {
                    LOG.debug("Cleaning up {}", p);
                    fsOps.deleteIfExists(p.toFile());
                }
            }
        }
    }

    @Override
    public void completelyRemove() throws IOException {
        removeAll(type.getFileName());
        if (type.needsExtraction()) {
            removeAll(type.getExtractionDir());
        }
        touch();
    }

    private void removeAll(String baseName) throws IOException {
        try (DirectoryStream<Path> children = fsOps.newDirectoryStream(topologyBasicBlobsRootDir)) {
            for (Path p : children) {
                String fileName = p.getFileName().toString();
                if (fileName.startsWith(baseName)) {
                    fsOps.deleteIfExists(p.toFile());
                }
            }
        }
    }

    @Override
    public long getSizeOnDisk() {
        return size;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof LocallyCachedTopologyBlob) {
            LocallyCachedTopologyBlob o = (LocallyCachedTopologyBlob)other;
            return topologyId.equals(o.topologyId) && type == o.type && topologyBasicBlobsRootDir.equals(o.topologyBasicBlobsRootDir);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return topologyId.hashCode() + type.hashCode() + topologyBasicBlobsRootDir.hashCode();
    }

    @Override
    public String toString() {
        return "LOCAL TOPO BLOB " + type + " " + topologyId;
    }
}
