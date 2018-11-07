package com.asa.lab.internalimp.datasource.relation.dataset;

import com.asa.lab.structure.datasource.ColumnDataSet;
import com.asa.lab.structure.datasource.DataSet;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 */
public class RelationDataSet implements DataSet {

    private ColumnDataSet[] columnDataSets;

    private int rowSize;

    public RelationDataSet(ColumnDataSet[] columnDataSets, int rowSize) {

        this.columnDataSets = columnDataSets;
        this.rowSize = rowSize;
    }

    @Override
    public Object getObject(int row, int column) {

        return columnDataSets[column].getObject(row);
    }

    @Override
    public int getColumnSize() {

        return columnDataSets.length;
    }

    @Override
    public int getRowSize() {

        return rowSize;
    }

    @Override
    public boolean getBoolean(int row, int column) {

        return false;
    }

    @Override
    public int getInt(int row, int column) {

        return 0;
    }

    @Override
    public long getLong(int row, int column) {

        return 0;
    }

    @Override
    public float getFloat(int row, int column) {

        return 0;
    }

    @Override
    public double getDouble(int row, int column) {

        return 0;
    }

    @Override
    public String getString(int row, int column) {

        return null;
    }

    @Override
    public Date getDate(int row, int column) {

        return null;
    }

    @Override
    public Timestamp getTimestamp(int row, int column) {

        return null;
    }
}
