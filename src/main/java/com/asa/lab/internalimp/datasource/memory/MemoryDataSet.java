package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.RowSet;

import java.util.Arrays;
import java.util.List;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryDataSet implements DataSet {


    private RowSet[] data;

    public MemoryDataSet() {

    }

    @Override
    public RowSet[] getDataArray() {

        return data;
    }

    @Override
    public List<RowSet> getDataList() {

        return Arrays.asList(data);
    }

    @Override
    public int size() {

        return data != null ? data.length : 0;
    }

    @Override
    public Object getObject(int row, int column) {

        return data[row].get(column);
    }

    public void setData(RowSet[] data) {

        this.data = data;
    }

    @Override
    public String toString() {

        return "MemoryDataSet{" +
                "data=" + Arrays.toString(data) +
                '}';
    }
}
