package com.asa.lab.internalimp.datasource;

import com.asa.lab.structure.datasource.DataSource;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/11.
 */
public class DataBaseContent {

    private Map<String, DataSource> dataSourceMap;

    private static DataBaseContent INSTANCE;

    private DataBaseContent() {

        dataSourceMap = new HashMap<>();
    }

    public static DataBaseContent getInstance() {

        if (INSTANCE == null) {
            synchronized (DataBaseContent.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DataBaseContent();
                }
            }
        }
        return INSTANCE;
    }

    public Map<String, DataSource> getDataSourceMap() {

        return dataSourceMap;
    }

    public void setDataSourceMap(Map<String, DataSource> dataSourceMap) {

        this.dataSourceMap = dataSourceMap;
    }

    public void addDataSource(String tableName, DataSource dataSource) {

        MapUtils.safeAddToMap(dataSourceMap, tableName, dataSource);
    }

    public DataSource getDataSource(String tableName) {

        return (DataSource) MapUtils.getObject(dataSourceMap, tableName, DataSourceHelper.emptyMemoryDataSource());
    }


}
