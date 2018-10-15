package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.RowSet;
import com.asa.utils.AssistUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.sql.Date;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryRowSet implements RowSet {

    private Object[] data;

    private Column[] columns;

    public MemoryRowSet(Column[] columns, Object[] data) {

        this.data = data;
    }

    @Override
    public String getString(int i) {

        return null;
    }

    @Override
    public double getDouble(int i) {

        return 0;
    }

    @Override
    public byte[] getBinary(int ordinal) {

        return new byte[0];
    }

    @Override
    public BigDecimal getDecimal(int i) {

        return null;
    }

    @Override
    public int getInt(int i) {

        return 0;
    }

    @Override
    public long getLong(int i) {

        return 0;
    }

    @Override
    public float getFloat(int ordinal) {

        return 0;
    }

    @Override
    public Date getDate(int i) {

        return null;
    }

    @Override
    public boolean isNullAt(int ordinal) {

        return false;
    }

    @Override
    public boolean getBoolean(int ordinal) {

        return false;
    }

    @Override
    public byte getByte(int ordinal) {

        return 0;
    }

    @Override
    public short getShort(int ordinal) {

        return 0;
    }

    @Override
    public int size() {

        return data != null ? data.length : 0;
    }

    @Override
    public Object apply(int i) {

        return data[i];
    }

    @Override
    public Object get(int i) {

        return data[i];
    }

    public Object[] getData() {

        return data;
    }

    public void setData(Object[] data) {

        this.data = data;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof MemoryRowSet)) {
            return false;
        }

        MemoryRowSet that = (MemoryRowSet) o;
        return AssistUtils.equals(data, that.data);
    }

    @Override
    public int hashCode() {

        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {

        return "MemoryRowSet{" +
                "data=" + Arrays.toString(data) +
                '}';
    }

    @Override
    public Object[] toArrary() {

        return data;
    }

    @Override
    public Column[] getColumns() {

        return columns;
    }

    public void setColumns(Column[] columns) {

        this.columns = columns;
    }
}
