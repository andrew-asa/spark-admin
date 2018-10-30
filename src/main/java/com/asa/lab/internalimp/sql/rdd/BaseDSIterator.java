package com.asa.lab.internalimp.sql.rdd;

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

    private Iterator<RowSet> iterator;

    private DataSource dataSource;

    private ComputeOption option;

    public BaseDSIterator(DataSource dataSource, Iterator<RowSet> iterator, ComputeOption option) {

        this.dataSource = dataSource;
        this.iterator = iterator;
        this.option = option;
    }

    @Override
    public boolean hasNext() {

        return iterator.hasNext();
    }

    @Override
    public BaseInternalRow next() {

        RowSet rowSet = iterator.next();
        return new BaseInternalRow(dataSource, rowSet, option);
    }
}
