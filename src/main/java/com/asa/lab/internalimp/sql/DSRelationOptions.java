package com.asa.lab.internalimp.sql;

import scala.Tuple2;
import scala.collection.Iterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 * 分布式表参数
 */
public class DSRelationOptions implements Serializable {

    private scala.collection.immutable.Map<String, String> param;

    private Map<String, String> parametersMap = new HashMap<>();

    public DSRelationOptions(scala.collection.immutable.Map<String, String> param) {

        this.param = param;
        init();
    }

    public void init() {

        Iterator<Tuple2<String, String>> iterator = param.iterator();
        while (iterator.hasNext()) {
            Tuple2<String, String> next = iterator.next();
            parametersMap.put(next._1().toLowerCase(), next._2());
        }
    }

    public String getValue(String key) {

        return parametersMap.get(key.toLowerCase());
    }

    public String getValue(Enum key) {

        return parametersMap.get(key.name().toLowerCase(Locale.ROOT));
    }
}
