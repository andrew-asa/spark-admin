package com.asa.lab.internalimp.datasource.spark;

import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.RowSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
    public Object getObject(int row, int column) {

        Row rowData = rowList.get(row);
        return rowData.get(column);
    }

    @Override
    public int getColumnSize() {

        return dataSchema.getColumns().length;
    }

    @Override
    public int getRowSize() {

        return rowList.size();
    }

    @Override
    public boolean getBoolean(int row, int column) {

        return rowList.get(row).getBoolean(column);
    }

    @Override
    public int getInt(int row, int column) {

        return rowList.get(row).getInt(column);
    }

    @Override
    public long getLong(int row, int column) {

        return rowList.get(row).getLong(column);
    }

    @Override
    public float getFloat(int row, int column) {

        return rowList.get(row).getFloat(column);
    }

    @Override
    public double getDouble(int row, int column) {

        return rowList.get(row).getDouble(column);
    }

    @Override
    public String getString(int row, int column) {

        return rowList.get(row).getString(column);
    }

    @Override
    public Date getDate(int row, int column) {

        return rowList.get(row).getDate(column);
    }

    @Override
    public Timestamp getTimestamp(int row, int column) {

        return rowList.get(row).getTimestamp(column);
    }
}
