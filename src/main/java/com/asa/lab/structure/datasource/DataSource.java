package com.asa.lab.structure.datasource;

/**
 * Created by andrew_asa on 2018/8/2.
 * 数据源
 */
public interface DataSource {

    String getName();

    void setName(String name);

    DataSet getDataSet();

    DataSchema getSchema();

    void setSchema(DataSchema schema);

    String getURIScheme();
}
