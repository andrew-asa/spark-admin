package com.asa.lab.structure.service.spark;

import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/10.
 * spark 配置
 */
public class SparkConfigure {

    private Map<String, Object> properties;

    public SparkConfigure() {

        properties = new HashMap<>();
    }

    public void setConfigure(String key, String value) {

        MapUtils.safeAddToMap(properties, key, value);
    }

    public String getMaster() {

        return MapUtils.getString(properties, SparkConstant.MASTER, SparkConstant.DEFAULT_MASTER);
    }

    public String getAppName() {

        return MapUtils.getString(properties, SparkConstant.APPNAME, SparkConstant.DEFAULT_APP_NAME);
    }

    public Object getPropertie(String key) {

        return properties.get(key);
    }

    public Object getPropertie(String key, Object defaultValue) {

        return MapUtils.getObject(properties, key, defaultValue);
    }

    public Map<String, Object> getProperties() {

        return properties;
    }

    public void setProperties(Map<String, Object> properties) {

        this.properties = properties;
    }
}
