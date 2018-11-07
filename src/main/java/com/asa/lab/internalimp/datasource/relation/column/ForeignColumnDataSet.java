package com.asa.lab.internalimp.datasource.relation.column;

import com.asa.lab.structure.datasource.ColumnDataSet;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.relation.RelationIndexMapping;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 * 小表列
 */
public class ForeignColumnDataSet implements ColumnDataSet {

    private DataSet dataSet;

    private int colIndex;

    private RelationIndexMapping indexMapping;

    public ForeignColumnDataSet(DataSet dataSet, int colIndex, RelationIndexMapping indexMapping) {

        this.dataSet = dataSet;
        this.colIndex = colIndex;
        this.indexMapping = indexMapping;
    }

    public DataSet getDataSet() {

        return dataSet;
    }

    public void setDataSet(DataSet dataSet) {

        this.dataSet = dataSet;
    }

    public int getColIndex() {

        return colIndex;
    }

    public void setColIndex(int colIndex) {

        this.colIndex = colIndex;
    }

    public RelationIndexMapping getIndexMapping() {

        return indexMapping;
    }

    public void setIndexMapping(RelationIndexMapping indexMapping) {

        this.indexMapping = indexMapping;
    }

    @Override
    public Object getObject(int rowIndex) {

        int index = indexMapping.getForeignRowIndex(rowIndex);
        if (index >= 0) {
            return dataSet.getObject(index, colIndex);
        } else {
            return null;
        }
    }
}
