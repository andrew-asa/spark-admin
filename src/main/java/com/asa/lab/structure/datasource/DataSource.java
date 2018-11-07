package com.asa.lab.structure.datasource;

import java.io.Serializable;

/**
 * Created by andrew_asa on 2018/8/2.
 * 数据源
 */
public interface DataSource extends Serializable {

    String getName();

    void setName(String name);

    DataSet getDataSet();

    DataSchema getSchema();

    void setSchema(DataSchema schema);

    String getURIScheme();
}
