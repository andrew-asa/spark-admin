package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.resultset.DataSet;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryDatasource implements DataSource {

    private MemoryDataSet source;

    public MemoryDatasource() {

    }

    public void setSource(MemoryDataSet source) {

        this.source = source;
    }

    @Override
    public DataSet getSource() {

        return source;
    }
}
