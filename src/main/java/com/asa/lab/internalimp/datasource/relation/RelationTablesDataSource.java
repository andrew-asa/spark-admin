package com.asa.lab.internalimp.datasource.relation;

import com.asa.lab.internalimp.datasource.relation.dataset.RelationDataSet;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.DataSource;

/**
 * @author andrew_asa
 * @date 2018/11/6.
 * 处理多关联表
 */
public class RelationTablesDataSource implements DataSource {

    private RelationDataSet dataSet;

    private DataSchema schema;

    public static final String URLSCHMA = "RelationTablesDataSource";

    public RelationTablesDataSource(RelationDataSet dataSet, DataSchema schema) {

        this.dataSet = dataSet;
        this.schema = schema;
    }

    @Override
    public String getName() {

        return null;
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public DataSet getDataSet() {

        return dataSet;
    }

    @Override
    public DataSchema getSchema() {

        return schema;
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
