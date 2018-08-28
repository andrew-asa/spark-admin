package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.structure.resultset.Row;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryRow implements Row {

    private Object[] data;

    @Override
    public String getString(int i) {

        return null;
    }

    @Override
    public double getDouble(int i) {

        return 0;
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
    public Date getDate(int i) {

        return null;
    }

    @Override
    public int size() {

        return data != null ? data.length : 0;
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
        if (!(o instanceof MemoryRow)) {
            return false;
        }

        MemoryRow row = (MemoryRow) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(data, row.data);
    }

    @Override
    public int hashCode() {

        return Arrays.hashCode(data);
    }
}
