package com.asa.lab.internalimp.datasource.relation.column;

import com.asa.lab.structure.datasource.ColumnDataSet;
import com.asa.lab.structure.datasource.DataSet;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 * 大表列
 */
public class PrimaryColumnDataSet implements ColumnDataSet {

    private DataSet dataSet;

    private int colIndex;

    public PrimaryColumnDataSet(DataSet dataSet, int colIndex) {

        this.dataSet = dataSet;
        this.colIndex = colIndex;
    }

    @Override
    public Object getObject(int rowIndex) {

        return dataSet.getObject(rowIndex, colIndex);
    }
}
