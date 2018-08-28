package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.structure.resultset.DataSet;
import com.asa.lab.structure.resultset.Row;
import com.asa.lab.structure.resultset.Type;

import java.util.Arrays;
import java.util.List;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryDataSet implements DataSet {

    private Type[] types;

    private Row[] data;

    public MemoryDataSet() {

    }

    @Override
    public Type[] getTypes() {

        return types;
    }

    public void setTypes(Type[] types) {

        this.types = types;
    }

    @Override
    public Row[] getDataArrary() {

        return data;
    }

    @Override
    public List<Row> getDataList() {

        return Arrays.asList(data);
    }

    @Override
    public int size() {

        return data != null ? data.length : 0;
    }

    public void setData(Row[] data) {

        this.data = data;
    }
}
