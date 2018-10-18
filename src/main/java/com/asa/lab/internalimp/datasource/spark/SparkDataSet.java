package com.asa.lab.internalimp.datasource.spark;

import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.RowSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/14.
 */
public class SparkDataSet implements DataSet {

    private DataSchema dataSchema;

    private Dataset<Row> rowDataset;

    private List<Row> rowList;

    public SparkDataSet() {

    }

    public SparkDataSet(DataSchema schema, Dataset<Row> rowDataset) {

        this.dataSchema = schema;
        this.rowDataset = rowDataset;
        rowList = rowDataset.collectAsList();
    }

    @Override
    public RowSet[] getDataArray() {

        return new RowSet[0];
    }

    @Override
    public List<RowSet> getDataList() {

        return null;
    }

    @Override
    public int size() {

        return rowList.size();
    }

    @Override
    public Object getObject(int row, int column) {

        Row rowData = rowList.get(row);
        return rowData.get(column);
    }
}
