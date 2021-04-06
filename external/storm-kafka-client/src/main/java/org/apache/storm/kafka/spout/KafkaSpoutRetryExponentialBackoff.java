/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link KafkaSpoutRetryService} using the exponential backoff formula. The time of the nextRetry is set as follows:
 * nextRetry = failCount == 1 ? currentTime + initialDelay : currentTime + delayPeriod*2^(failCount-1)    where failCount = 1, 2, 3, ...
 * nextRetry = Min(nextRetry, currentTime + maxDelay)
 */
public class KafkaSpoutRetryExponentialBackoff implements KafkaSpoutRetryService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutRetryExponentialBackoff.class);
    private static final RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();

    private final TimeInterval initialDelay;
    private final TimeInterval delayPeriod;
    private final TimeInterval maxDelay;
    private final int maxRetries;

    //This class assumes that there is at most one retry schedule per message id in this set at a time.
    private final Set<RetrySchedule> retrySchedules = new TreeSet<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
    private final Set<KafkaSpoutMessageId> toRetryMsgs = new HashSet<>();      // Convenience data structure to speedup lookups

    /**
     * Comparator ordering by timestamp. 
     */
    private static class RetryEntryTimeStampComparator implements Serializable, Comparator<RetrySchedule> {
        @Override
        public int compare(RetrySchedule entry1, RetrySchedule entry2) {
            int result = Long.valueOf(entry1.nextRetryTimeNanos()).compareTo(entry2.nextRetryTimeNanos());
            
            if (result == 0) {
                //TreeSet uses compareTo instead of equals() for the Set contract
                //Ensure that we can save two retry schedules with the same timestamp
                result = entry1.hashCode() - entry2.hashCode();
            }
            return result;
        }
    }

    private class RetrySchedule {
        private final KafkaSpoutMessageId msgId;
        private long nextRetryTimeNanos;

        public RetrySchedule(KafkaSpoutMessageId msgId, long nextRetryTimeNanos) {
            this.msgId = msgId;
            this.nextRetryTimeNanos = nextRetryTimeNanos;
            LOG.debug("Created {}", this);
        }

        public void setNextRetryTimeNanos() {
            nextRetryTimeNanos = nextTime(msgId);
            LOG.debug("Updated {}", this);
        }

        public boolean retry(long currentTimeNanos) {
            return nextRetryTimeNanos <= currentTimeNanos;
        }

        @Override
        public String toString() {
            return "RetrySchedule{"
                    + "msgId=" + msgId
                    + ", nextRetryTimeNanos=" + nextRetryTimeNanos
                    + '}';
        }

        public KafkaSpoutMessageId msgId() {
            return msgId;
        }

        public long nextRetryTimeNanos() {
            return nextRetryTimeNanos;
        }
    }

    public static class TimeInterval implements Serializable {
        private final long lengthNanos;
        private final TimeUnit timeUnit;
        private final long length;

        /**
         * Creates a new TimeInterval.
         * @param length length of the time interval in the units specified by {@link TimeUnit}
         * @param timeUnit unit used to specify a time interval on which to specify a time unit
         */
        public TimeInterval(long length, TimeUnit timeUnit) {
            this.lengthNanos = timeUnit.toNanos(length);
            this.timeUnit = timeUnit;
            this.length = length;
        }
        
        public static TimeInterval seconds(long length) {
            return new TimeInterval(length, TimeUnit.SECONDS);
        }

        public static TimeInterval milliSeconds(long length) {
            return new TimeInterval(length, TimeUnit.MILLISECONDS);
        }
        
        public static TimeInterval microSeconds(long length) {
            return new TimeInterval(length, TimeUnit.MICROSECONDS);
        }
        
        public long lengthNanos() {
            return lengthNanos;
        }
        
        public TimeUnit timeUnit() {
            return timeUnit;
        }

        @Override
        public String toString() {
            return "TimeInterval{"
                    + "length=" + length
                    + ", timeUnit=" + timeUnit
                    + '}';
        }
    }

    /**
     * The time stamp of the next retry is scheduled according to the exponential backoff formula (geometric progression):
     * nextRetry = failCount == 1 ? currentTime + initialDelay : currentTime + delayPeriod^(failCount-1),
     * where failCount = 1, 2, 3, ... nextRetry = Min(nextRetry, currentTime + maxDelay).
     * <p/>
     * By specifying a value for maxRetries lower than Integer.MAX_VALUE, the user decides to sacrifice guarantee of delivery for the
     * previous polled records in favor of processing more records.
     *
     * @param initialDelay      initial delay of the first retry
     * @param delayPeriod       the time interval that is the ratio of the exponential backoff formula (geometric progression)
     * @param maxRetries        maximum number of times a tuple is retried before being acked and scheduled for commit
     * @param maxDelay          maximum amount of time waiting before retrying
     *
     */
    public KafkaSpoutRetryExponentialBackoff(TimeInterval initialDelay, TimeInterval delayPeriod, int maxRetries, TimeInterval maxDelay) {
        this.initialDelay = initialDelay;
        this.delayPeriod = delayPeriod;
        this.maxRetries = maxRetries;
        this.maxDelay = maxDelay;
        LOG.debug("Instantiated {}", this.toStringImpl());
    }

    @Override
    public Map<TopicPartition, Long> earliestRetriableOffsets() {
        final Map<TopicPartition, Long> tpToEarliestRetriableOffset = new HashMap<>();
        final long currentTimeNanos = Time.nanoTime();
        for (RetrySchedule retrySchedule : retrySchedules) {
            if (retrySchedule.retry(currentTimeNanos)) {
                final KafkaSpoutMessageId msgId = retrySchedule.msgId;
                final TopicPartition tpForMessage = new TopicPartition(msgId.topic(), msgId.partition());
                tpToEarliestRetriableOffset.merge(tpForMessage, msgId.offset(), Math::min);
            } else {
                break;  // Stop searching as soon as passed current time
            }
        }
        LOG.debug("Topic partitions with entries ready to be retried [{}] ", tpToEarliestRetriableOffset);
        return tpToEarliestRetriableOffset;
    }

    @Override
    public boolean isReady(KafkaSpoutMessageId msgId) {
        boolean retry = false;
        if (isScheduled(msgId)) {
            final long currentTimeNanos = Time.nanoTime();
            for (RetrySchedule retrySchedule : retrySchedules) {
                if (retrySchedule.retry(currentTimeNanos)) {
                    if (retrySchedule.msgId.equals(msgId)) {
                        retry = true;
                        LOG.debug("Found entry to retry {}", retrySchedule);
                        break; //Stop searching if the message is known to be ready for retry
                    }
                } else {
                    LOG.debug("Entry to retry not found {}", retrySchedule);
                    break;  // Stop searching as soon as passed current time
                }
            }
        }
        return retry;
    }

    @Override
    public boolean isScheduled(KafkaSpoutMessageId msgId) {
        return toRetryMsgs.contains(msgId);
    }

    @Override
    public boolean remove(KafkaSpoutMessageId msgId) {
        boolean removed = false;
        if (isScheduled(msgId)) {
            toRetryMsgs.remove(msgId);
            for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                final RetrySchedule retrySchedule = iterator.next();
                if (retrySchedule.msgId().equals(msgId)) {
                    iterator.remove();
                    removed = true;
                    break; //There is at most one schedule per message id
                }
            }
        }
        LOG.debug(removed ? "Removed {} " : "Not removed {}", msgId);
        LOG.trace("Current state {}", retrySchedules);
        return removed;
    }

    @Override
    public boolean retainAll(Collection<TopicPartition> topicPartitions) {
        boolean result = false;
        for (Iterator<RetrySchedule> rsIterator = retrySchedules.iterator(); rsIterator.hasNext(); ) {
            final RetrySchedule retrySchedule = rsIterator.next();
            final KafkaSpoutMessageId msgId = retrySchedule.msgId;
            final TopicPartition tpRetry = new TopicPartition(msgId.topic(), msgId.partition());
            if (!topicPartitions.contains(tpRetry)) {
                rsIterator.remove();
                toRetryMsgs.remove(msgId);
                LOG.debug("Removed {}", retrySchedule);
                LOG.trace("Current state {}", retrySchedules);
                result = true;
            }
        }
        return result;
    }

    @Override
    public boolean schedule(KafkaSpoutMessageId msgId) {
        if (msgId.numFails() > maxRetries) {
            LOG.debug("Not scheduling [{}] because reached maximum number of retries [{}].", msgId, maxRetries);
            return false;
        } else {
            //Remove existing schedule for the message id
            remove(msgId);
            final RetrySchedule retrySchedule = new RetrySchedule(msgId, nextTime(msgId));
            retrySchedules.add(retrySchedule);
            toRetryMsgs.add(msgId);
            LOG.debug("Scheduled. {}", retrySchedule);
            LOG.trace("Current state {}", retrySchedules);
            return true;
        }
    }
    
    @Override
    public int readyMessageCount() {
        int count = 0;
        final long currentTimeNanos = Time.nanoTime();
        for (RetrySchedule retrySchedule : retrySchedules) {
            if (retrySchedule.retry(currentTimeNanos)) {
                ++count;
            } else {
                break; //Stop counting when past current time
            }
        }
        return count;
    }

    @Override
    public KafkaSpoutMessageId getMessageId(TopicPartition tp, long offset) {
        KafkaSpoutMessageId msgId = new KafkaSpoutMessageId(tp, offset);
        if (isScheduled(msgId)) {
            for (KafkaSpoutMessageId originalMsgId : toRetryMsgs) {
                if (originalMsgId.equals(msgId)) {
                    return originalMsgId;
                }
            }
        }
        return msgId;
    }

    // if value is greater than Long.MAX_VALUE it truncates to Long.MAX_VALUE
    private long nextTime(KafkaSpoutMessageId msgId) {
        Validate.isTrue(msgId.numFails() > 0, "nextTime assumes the message has failed at least once");
        final long currentTimeNanos = Time.nanoTime();
        final long nextTimeNanos = msgId.numFails() == 1                // numFails = 1, 2, 3, ...
                ? currentTimeNanos + initialDelay.lengthNanos
                : currentTimeNanos + delayPeriod.lengthNanos * (long)(Math.pow(2, msgId.numFails() - 1));
        return Math.min(nextTimeNanos, currentTimeNanos + maxDelay.lengthNanos);
    }

    @Override
    public String toString() {
        return toStringImpl();
    }
    
    private String toStringImpl() {
        //This is here to avoid an overridable call in the constructor
        return "KafkaSpoutRetryExponentialBackoff{"
                + "delay=" + initialDelay
                + ", ratio=" + delayPeriod
                + ", maxRetries=" + maxRetries
                + ", maxRetryDelay=" + maxDelay
                + '}';
    }
}
