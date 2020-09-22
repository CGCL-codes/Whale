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

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Map;
import org.apache.storm.Constants;
import org.apache.storm.generated.WorkerResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resources that have been normalized. This class is intended as a delegate for more specific types of normalized resource set, since it
 * does not keep track of memory as a resource.
 */
public class NormalizedResources {

    private static final Logger LOG = LoggerFactory.getLogger(NormalizedResources.class);

    public static ResourceNameNormalizer RESOURCE_NAME_NORMALIZER;
    private static ResourceMapArrayBridge RESOURCE_MAP_ARRAY_BRIDGE;

    static {
        resetResourceNames();
    }

    private double cpu;
    private double[] otherResources;

    /**
     * This is for testing only. It allows a test to reset the static state relating to resource names. We reset the mapping because some
     * algorithms sadly have different behavior if a resource exists or not.
     */
    @VisibleForTesting
    public static void resetResourceNames() {
        RESOURCE_NAME_NORMALIZER = new ResourceNameNormalizer();
        RESOURCE_MAP_ARRAY_BRIDGE = new ResourceMapArrayBridge();
    }

    /**
     * Copy constructor.
     */
    public NormalizedResources(NormalizedResources other) {
        cpu = other.cpu;
        otherResources = Arrays.copyOf(other.otherResources, other.otherResources.length);
    }

    /**
     * Create a new normalized set of resources. Note that memory is not managed by this class, as it is not consistent in requests vs
     * offers because of how on heap vs off heap is used.
     *
     * @param normalizedResources the normalized resource map
     */
    public NormalizedResources(Map<String, Double> normalizedResources) {
        cpu = normalizedResources.getOrDefault(Constants.COMMON_CPU_RESOURCE_NAME, 0.0);
        otherResources = RESOURCE_MAP_ARRAY_BRIDGE.translateToResourceArray(normalizedResources);
    }

    /**
     * Get the total amount of cpu.
     *
     * @return the amount of cpu.
     */
    public double getTotalCpu() {
        return cpu;
    }
    
    private void zeroPadOtherResourcesIfNecessary(int requiredLength) {
        if (requiredLength > otherResources.length) {
            double[] newResources = new double[requiredLength];
            System.arraycopy(otherResources, 0, newResources, 0, otherResources.length);
            otherResources = newResources;
        }
    }

    private void add(double[] resourceArray) {
        int otherLength = resourceArray.length;
        zeroPadOtherResourcesIfNecessary(otherLength);
        for (int i = 0; i < otherLength; i++) {
            otherResources[i] += resourceArray[i];
        }
    }

    public void add(NormalizedResources other) {
        this.cpu += other.cpu;
        add(other.otherResources);
    }

    /**
     * Add the resources from a worker to this.
     *
     * @param value the worker resources that should be added to this.
     */
    public void add(WorkerResources value) {
        Map<String, Double> workerNormalizedResources = value.get_resources();
        cpu += workerNormalizedResources.getOrDefault(Constants.COMMON_CPU_RESOURCE_NAME, 0.0);
        add(RESOURCE_MAP_ARRAY_BRIDGE.translateToResourceArray(workerNormalizedResources));
    }

    /**
     * Throw an IllegalArgumentException because a resource became negative during remove.
     * @param resourceName The name of the resource that became negative
     * @param currentValue The current value of the resource
     * @param subtractedValue The value that was subtracted to make the resource negative
     */
    public void throwBecauseResourceBecameNegative(String resourceName, double currentValue, double subtractedValue) {
        throw new IllegalArgumentException(String.format("Resource amounts should never be negative."
            + " Resource '%s' with current value '%f' became negative because '%f' was removed.",
            resourceName, currentValue, subtractedValue));
    }
    
    /**
     * Remove the other resources from this. This is the same as subtracting the resources in other from this.
     *
     * @param other the resources we want removed.
     * @throws IllegalArgumentException if subtracting other from this would result in any resource amount becoming negative.
     */
    public void remove(NormalizedResources other) {
        this.cpu -= other.cpu;
        if (cpu < 0.0) {
            throwBecauseResourceBecameNegative(Constants.COMMON_CPU_RESOURCE_NAME, cpu, other.cpu);
        }
        int otherLength = other.otherResources.length;
        zeroPadOtherResourcesIfNecessary(otherLength);
        for (int i = 0; i < otherLength; i++) {
            otherResources[i] -= other.otherResources[i];
            if (otherResources[i] < 0.0) {
                throwBecauseResourceBecameNegative(getResourceNameForResourceIndex(i), otherResources[i], other.otherResources[i]);
            }
        }
    }

    @Override
    public String toString() {
        return "Normalized resources: " + toNormalizedMap();
    }

    /**
     * Return a Map of the normalized resource name to a double. This should only be used when returning thrift resource requests to the end
     * user.
     */
    public Map<String, Double> toNormalizedMap() {
        Map<String, Double> ret = RESOURCE_MAP_ARRAY_BRIDGE.translateFromResourceArray(otherResources);
        ret.put(Constants.COMMON_CPU_RESOURCE_NAME, cpu);
        return ret;
    }

    private double getResourceAt(int index) {
        if (index >= otherResources.length) {
            return 0.0;
        }
        return otherResources[index];
    }

    /**
     * A simple sanity check to see if all of the resources in this would be large enough to hold the resources in other ignoring memory. It
     * does not check memory because with shared memory it is beyond the scope of this.
     *
     * @param other the resources that we want to check if they would fit in this.
     * @param thisTotalMemoryMb The total memory in MB of this
     * @param otherTotalMemoryMb The total memory in MB of other
     * @return true if it might fit, else false if it could not possibly fit.
     */
    public boolean couldHoldIgnoringSharedMemory(NormalizedResources other, double thisTotalMemoryMb, double otherTotalMemoryMb) {
        if (this.cpu < other.getTotalCpu()) {
            return false;
        }
        int length = Math.max(this.otherResources.length, other.otherResources.length);
        for (int i = 0; i < length; i++) {
            if (getResourceAt(i) < other.getResourceAt(i)) {
                return false;
            }
        }

        return thisTotalMemoryMb >= otherTotalMemoryMb;
    }

    private String getResourceNameForResourceIndex(int resourceIndex) {
        for (Map.Entry<String, Integer> entry : RESOURCE_MAP_ARRAY_BRIDGE.getResourceNamesToArrayIndex().entrySet()) {
            int index = entry.getValue();
            if (index == resourceIndex) {
                return entry.getKey();
            }
        }
        return null;
    }

    private void throwBecauseUsedIsNotSubsetOfTotal(NormalizedResources used, double totalMemoryMb, double usedMemoryMb) {
        throw new IllegalArgumentException(String.format("The used resources must be a subset of the total resources."
            + " Used: '%s', Total: '%s', Used Mem: '%f', Total Mem: '%f'",
            used.toNormalizedMap(), this.toNormalizedMap(), usedMemoryMb, totalMemoryMb));
    }

    /**
     * Calculate the average resource usage percentage with this being the total resources and used being the amounts used. Used must be a
     * subset of the total resources. If a resource in the total has a value of zero, it will be skipped in the calculation to avoid
     * division by 0. If all resources are skipped the result is defined to be 100.0.
     *
     * @param used the amount of resources used.
     * @param totalMemoryMb The total memory in MB
     * @param usedMemoryMb The used memory in MB
     * @return the average percentage used 0.0 to 100.0.
     * @throws IllegalArgumentException if any resource in used has a greater value than the same resource in the total, or used has generic
     *     resources that are not present in the total.
     */
    public double calculateAveragePercentageUsedBy(NormalizedResources used, double totalMemoryMb, double usedMemoryMb) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Calculating avg percentage used by. Used Mem: {} Total Mem: {}"
                + " Used Normalized Resources: {} Total Normalized Resources: {}", totalMemoryMb, usedMemoryMb,
                toNormalizedMap(), used.toNormalizedMap());
        }

        int skippedResourceTypes = 0;
        double total = 0.0;
        if (usedMemoryMb > totalMemoryMb) {
            throwBecauseUsedIsNotSubsetOfTotal(used, totalMemoryMb, usedMemoryMb);
        }
        if (totalMemoryMb != 0.0) {
            total += usedMemoryMb / totalMemoryMb;
        } else {
            skippedResourceTypes++;
        }
        double totalCpu = getTotalCpu();
        if (used.getTotalCpu() > getTotalCpu()) {
            throwBecauseUsedIsNotSubsetOfTotal(used, totalMemoryMb, usedMemoryMb);
        }
        if (totalCpu != 0.0) {
            total += used.getTotalCpu() / getTotalCpu();
        } else {
            skippedResourceTypes++;
        }

        if (used.otherResources.length > otherResources.length) {
            throwBecauseUsedIsNotSubsetOfTotal(used, totalMemoryMb, usedMemoryMb);
        }

        for (int i = 0; i < otherResources.length; i++) {
            double totalValue = otherResources[i];
            double usedValue;
            if (i >= used.otherResources.length) {
                //Resources missing from used are using none of that resource
                usedValue = 0.0;
            } else {
                usedValue = used.otherResources[i];
            }
            if (usedValue > totalValue) {
                throwBecauseUsedIsNotSubsetOfTotal(used, totalMemoryMb, usedMemoryMb);
            }
            if (totalValue == 0.0) {
                //Skip any resources where the total is 0, the percent used for this resource isn't meaningful.
                //We fall back to prioritizing by cpu, memory and any other resources by ignoring this value
                skippedResourceTypes++;
                continue;
            }

            total += usedValue / totalValue;
        }
        //Adjust the divisor for the average to account for any skipped resources (those where the total was 0)
        int divisor = 2 + otherResources.length - skippedResourceTypes;
        if (divisor == 0) {
            /*
             * This is an arbitrary choice to make the result consistent with calculateMin. Any value would be valid here, becase there are
             * no (non-zero) resources in the total set of resources, so we're trying to average 0 values.
             */
            return 100.0;
        } else {
            return (total * 100.0) / divisor;
        }
    }

    /**
     * Calculate the minimum resource usage percentage with this being the total resources and used being the amounts used. Used must be a
     * subset of the total resources. If a resource in the total has a value of zero, it will be skipped in the calculation to avoid
     * division by 0. If all resources are skipped the result is defined to be 100.0.
     *
     * @param used the amount of resources used.
     * @param totalMemoryMb The total memory in MB
     * @param usedMemoryMb The used memory in MB
     * @return the minimum percentage used 0.0 to 100.0.
     * @throws IllegalArgumentException if any resource in used has a greater value than the same resource in the total, or used has generic
     *     resources that are not present in the total.
     */
    public double calculateMinPercentageUsedBy(NormalizedResources used, double totalMemoryMb, double usedMemoryMb) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Calculating min percentage used by. Used Mem: {} Total Mem: {}"
                + " Used Normalized Resources: {} Total Normalized Resources: {}", totalMemoryMb, usedMemoryMb,
                toNormalizedMap(), used.toNormalizedMap());
        }

        double min = 1.0;
        if (usedMemoryMb > totalMemoryMb) {
            throwBecauseUsedIsNotSubsetOfTotal(used, totalMemoryMb, usedMemoryMb);
        }
        if (totalMemoryMb != 0.0) {
            min = Math.min(min, usedMemoryMb / totalMemoryMb);
        }
        double totalCpu = getTotalCpu();
        if (used.getTotalCpu() > totalCpu) {
            throwBecauseUsedIsNotSubsetOfTotal(used, totalMemoryMb, usedMemoryMb);
        }
        if (totalCpu != 0.0) {
            min = Math.min(min, used.getTotalCpu() / totalCpu);
        }

        if (used.otherResources.length > otherResources.length) {
            throwBecauseUsedIsNotSubsetOfTotal(used, totalMemoryMb, usedMemoryMb);
        }
        
        for (int i = 0; i < otherResources.length; i++) {
            if (otherResources[i] == 0.0) {
                //Skip any resources where the total is 0, the percent used for this resource isn't meaningful.
                //We fall back to prioritizing by cpu, memory and any other resources by ignoring this value
                continue;
            }
            if (i >= used.otherResources.length) {
                //Resources missing from used are using none of that resource
                return 0;
            }
            if (used.otherResources[i] > otherResources[i]) {
                throwBecauseUsedIsNotSubsetOfTotal(used, totalMemoryMb, usedMemoryMb);
            }
            min = Math.min(min, used.otherResources[i] / otherResources[i]);
        }
        return min * 100.0;
    }
}
