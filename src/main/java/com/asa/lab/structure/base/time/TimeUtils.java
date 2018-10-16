package com.asa.lab.structure.base.time;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/16.
 */
public class TimeUtils {

    private static Map<TimeInterval, Long> intervalMillisMap;

    public static long intervalSeconds(TimeInterval timeInterval) {

        if (intervalMillisMap == null) {
            synchronized (TimeUtils.class) {
                if (intervalMillisMap == null) {
                    intervalMillisMap = new HashMap<>(8);
                    intervalMillisMap.put(TimeInterval.Second, 1L);
                    intervalMillisMap.put(TimeInterval.Minute, 60 * intervalMillisMap.get(TimeInterval.Second));
                    intervalMillisMap.put(TimeInterval.Hour, 60 * intervalMillisMap.get(TimeInterval.Minute));
                    intervalMillisMap.put(TimeInterval.Day, 24 * intervalMillisMap.get(TimeInterval.Hour));
                    intervalMillisMap.put(TimeInterval.Week, 7 * intervalMillisMap.get(TimeInterval.Day));
                    intervalMillisMap.put(TimeInterval.Month, 30 * intervalMillisMap.get(TimeInterval.Day));
                    intervalMillisMap.put(TimeInterval.Season, 3 * intervalMillisMap.get(TimeInterval.Month));
                    intervalMillisMap.put(TimeInterval.Year, 365 * intervalMillisMap.get(TimeInterval.Day));
                }
            }
        }
        return intervalMillisMap.get(timeInterval);
    }

}
