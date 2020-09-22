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

package org.apache.storm.scheduler.resource.normalization;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.utils.ObjectReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resource request with normalized resource names.
 */
public class NormalizedResourceRequest implements NormalizedResourcesWithMemory {

    private static final Logger LOG = LoggerFactory.getLogger(NormalizedResourceRequest.class);

    private static void putIfMissing(Map<String, Double> dest, String destKey, Map<String, Object> src, String srcKey) {
        if (!dest.containsKey(destKey)) {
            Number value = (Number) src.get(srcKey);
            if (value != null) {
                dest.put(destKey, value.doubleValue());
            }
        }
    }

    private static Map<String, Double> getDefaultResources(Map<String, Object> topoConf) {
        Map<String, Double> ret = NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap((Map<String, Number>) topoConf.getOrDefault(
            Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, new HashMap<>()));
        putIfMissing(ret, Constants.COMMON_CPU_RESOURCE_NAME, topoConf, Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
        putIfMissing(ret, Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, topoConf, Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
        putIfMissing(ret, Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, topoConf, Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
        return ret;
    }

    private static Map<String, Double> parseResources(String input) {
        Map<String, Double> topologyResources = new HashMap<>();
        JSONParser parser = new JSONParser();
        LOG.debug("Input to parseResources {}", input);
        try {
            if (input != null) {
                Object obj = parser.parse(input);
                JSONObject jsonObject = (JSONObject) obj;

                // Legacy resource parsing
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
                    Double topoMemOnHeap = ObjectReader
                        .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, topoMemOnHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
                    Double topoMemOffHeap = ObjectReader
                        .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, topoMemOffHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
                    Double topoCpu = ObjectReader.getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT),
                        null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topoCpu);
                }

                // If resource is also present in resources map will overwrite the above
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP)) {
                    Map<String, Number> rawResourcesMap =
                        (Map<String, Number>) jsonObject.computeIfAbsent(
                            Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, (k) -> new HashMap<>());

                    for (Map.Entry<String, Number> stringNumberEntry : rawResourcesMap.entrySet()) {
                        topologyResources.put(
                            stringNumberEntry.getKey(), stringNumberEntry.getValue().doubleValue());
                    }

                }
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse component resources is:" + e.toString(), e);
            return null;
        }
        return topologyResources;
    }

    private final NormalizedResources normalizedResources;
    private double onHeap;
    private double offHeap;

    private NormalizedResourceRequest(Map<String, ? extends Number> resources,
        Map<String, Double> defaultResources) {
        Map<String, Double> normalizedResourceMap = NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(defaultResources);
        normalizedResourceMap.putAll(NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resources));
        onHeap = normalizedResourceMap.getOrDefault(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, 0.0);
        offHeap = normalizedResourceMap.getOrDefault(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, 0.0);
        normalizedResources = new NormalizedResources(normalizedResourceMap);
    }

    public NormalizedResourceRequest(ComponentCommon component, Map<String, Object> topoConf) {
        this(parseResources(component.get_json_conf()), getDefaultResources(topoConf));
    }

    public NormalizedResourceRequest(Map<String, Object> topoConf) {
        this((Map<String, ? extends Number>) null, getDefaultResources(topoConf));
    }

    public NormalizedResourceRequest() {
        this((Map<String, ? extends Number>) null, null);
    }

    public Map<String, Double> toNormalizedMap() {
        Map<String, Double> ret = this.normalizedResources.toNormalizedMap();
        ret.put(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, offHeap);
        ret.put(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, onHeap);
        return ret;
    }

    public double getOnHeapMemoryMb() {
        return onHeap;
    }

    public void addOnHeap(final double onHeap) {
        this.onHeap += onHeap;
    }

    public double getOffHeapMemoryMb() {
        return offHeap;
    }

    public void addOffHeap(final double offHeap) {
        this.offHeap += offHeap;
    }

    /**
     * Add the resources in other to this.
     *
     * @param other the other Request to add to this.
     */
    public void add(NormalizedResourceRequest other) {
        this.normalizedResources.add(other.normalizedResources);
        onHeap += other.onHeap;
        offHeap += other.offHeap;
    }

    public void add(WorkerResources value) {
        this.normalizedResources.add(value);
        //The resources are already normalized
        Map<String, Double> resources = value.get_resources();
        onHeap += resources.getOrDefault(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, 0.0);
        offHeap += resources.getOrDefault(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, 0.0);
    }

    @Override
    public double getTotalMemoryMb() {
        return onHeap + offHeap;
    }

    @Override
    public String toString() {
        return super.toString() + " onHeap: " + onHeap + " offHeap: " + offHeap;
    }

    public double getTotalCpu() {
        return this.normalizedResources.getTotalCpu();
    }

    @Override
    public NormalizedResources getNormalizedResources() {
        return this.normalizedResources;
    }
}
