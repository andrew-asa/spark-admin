package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.internalimp.datasource.BaseDataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.RowSet;
import com.asa.utils.AssistUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryDataSet implements DataSet {


    private RowSet[] data;

    private BaseDataSchema schema;

    public MemoryDataSet(RowSet[] data, BaseDataSchema schema) {

        this.data = data;
        this.schema = schema;
    }

    @Override
    public Object getObject(int row, int column) {

        return data[row].get(column);
    }

    @Override
    public int getColumnSize() {

        return schema.getColumns().length;
    }

    @Override
    public int getRowSize() {

        return data.length;
    }

    @Override
    public boolean getBoolean(int row, int column) {

        return data[row].getBoolean(column);
    }

    @Override
    public int getInt(int row, int column) {

        return data[row].getInt(column);
    }

    @Override
    public long getLong(int row, int column) {

        return data[row].getLong(column);
    }

    @Override
    public float getFloat(int row, int column) {

        return data[row].getFloat(column);
    }

    @Override
    public double getDouble(int row, int column) {

        return data[row].getDouble(column);
    }

    @Override
    public String getString(int row, int column) {

        return data[row].getString(column);
    }

    @Override
    public Date getDate(int row, int column) {

        return data[row].getDate(column);
    }

    @Override
    public Timestamp getTimestamp(int row, int column) {

        return null;
    }

    public void setData(RowSet[] data) {

        this.data = data;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof MemoryDataSet)) {
            return false;
        }

        MemoryDataSet set = (MemoryDataSet) o;
        return AssistUtils.equals(data, set.data) &&
                AssistUtils.equals(schema, set.schema);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(data, schema);
    }
}
