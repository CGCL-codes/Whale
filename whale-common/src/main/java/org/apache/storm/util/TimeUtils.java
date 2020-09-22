package org.apache.storm.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * locate org.apache.storm.util
 * Created by master on 2019/10/15.
 */
public class TimeUtils {
    /**
     * waitForNanoSeconds等待时间
     * @param nanoSeconds
     */
    public static void waitForNanoSeconds(long nanoSeconds) {
        Long startNanoSeconds = System.nanoTime();
        while (true) {
            Long endNanoSeconds = System.nanoTime();
            if (endNanoSeconds - startNanoSeconds >= nanoSeconds)
                break;

        }
    }

    /**
     * waitForTimeMills等待时间
     * @param timeMills
     */
    public static void waitForTimeMills(long timeMills){
        if(timeMills!=0) {
            Long startTimeMllls = System.currentTimeMillis();
            while (true) {
                Long endTimeMills = System.currentTimeMillis();
                if ((endTimeMills - startTimeMllls) >= timeMills)
                    break;
            }
        }
    }

    private static final DateFormat defaultDateFormat = new SimpleDateFormat(
            "yyyyMMdd_HHmmss");

    public static String getTimestamp(Date date) {
        return defaultDateFormat.format(date);
    }

    public static String getTimestamp() {
        return getTimestamp(new Date());
    }

    public static String getTimestamp(long tsInMillis) {
        return getTimestamp(new Date(tsInMillis));
    }

    public static String getTimestamp(Date date, String format) {
        return (new SimpleDateFormat(format)).format(date);
    }

    public static String getTimestamp(String format) {
        return getTimestamp(new Date(), format);
    }

    public static String getTimestamp(long tsInMillis, String format) {
        return getTimestamp(new Date(tsInMillis), format);
    }
}
