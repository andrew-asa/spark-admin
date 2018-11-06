package com.asa.lab.internalimp.datasource;

import com.asa.lab.internalimp.datasource.relation.Relation;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.utils.ListUtils;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/11.
 */
public class DataBaseContent {

    private Map<String, DataSource> dataSourceMap;

    /**
     * 主表，子表 --> 关联
     */
    private Map<List<String>, Relation> relationMap;

    private static DataBaseContent INSTANCE;

    private DataBaseContent() {

        dataSourceMap = new HashMap<>();
        relationMap = new HashMap<>();
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

    /**
     * 添加一条关联
     *
     * @param relation
     */
    public void addRelation(Relation relation) {

        String primaryTable = relation.getPrimaryTable();
        String foreignTable = relation.getForeignTable();
        List<String> key = ListUtils.arrayToList(primaryTable, foreignTable);
        relationMap.put(key, relation);
    }

    public void removeRelation(String primaryTable, String foreignTable) {

        List<String> key = ListUtils.arrayToList(primaryTable, foreignTable);
        relationMap.remove(key);
    }

    public DataSource getDataSource(String tableName) {

        return (DataSource) MapUtils.getObject(dataSourceMap, tableName, DataSourceHelper.emptyMemoryDataSource());
    }
}
