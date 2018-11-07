package com.asa.lab.internalimp.sql.rdd;

import com.asa.lab.internalimp.datasource.memory.MemoryRowSet;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.RowSet;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.mutable.WrappedArray;

import java.util.Iterator;

/**
 * @author andrew_asa
 * @date 2018/10/12.
 */
public class BaseDSIterator extends AbstractDSIterator<BaseInternalRow> {


    private DataSource dataSource;

    private ComputeOption option;

    private int currentIndex;

    private int totalRowSize;

    private Column[] columns;

    private int totalColSize;

    public BaseDSIterator(DataSource dataSource, ComputeOption option) {

        this.dataSource = dataSource;
        this.option = option;
        init();
    }

    private void init() {

        totalRowSize = dataSource.getDataSet().getRowSize();
        currentIndex = 0;
        columns = dataSource.getSchema().getColumns();
        totalColSize = columns.length;
    }

    @Override
    public boolean hasNext() {

        return currentIndex < totalRowSize;
    }

    @Override
    public BaseInternalRow next() {

        DataSet dataSet = dataSource.getDataSet();
        Object[] rowData = new Object[totalColSize];
        for (int i = 0; i < totalColSize; i++) {
            rowData[i] = dataSet.getObject(currentIndex, i);
        }
        MemoryRowSet memoryRowSet = new MemoryRowSet(columns, rowData);
        currentIndex++;
        return new BaseInternalRow(dataSource, memoryRowSet, option);
    }
}
