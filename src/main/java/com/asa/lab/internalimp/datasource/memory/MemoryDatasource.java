package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.DataSet;

import java.io.Serializable;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryDatasource implements DataSource, Serializable {

    public static final String URLSCHMA = "memory";

    private DataSet dataSet;

    private DataSchema schema;

    private String name;

    public MemoryDatasource() {

    }

    @Override
    public String getName() {

        return name;
    }

    @Override
    public void setName(String name) {

        this.name = name;
    }

    @Override
    public DataSet getDataSet() {

        return dataSet;
    }

    @Override
    public DataSchema getSchema() {

        return schema;
    }

    public void setDataSet(DataSet dataSet) {

        this.dataSet = dataSet;
    }

    @Override
    public void setSchema(DataSchema schema) {

        this.schema = schema;
    }

    @Override
    public String getURIScheme() {

        return URLSCHMA;
    }
}
