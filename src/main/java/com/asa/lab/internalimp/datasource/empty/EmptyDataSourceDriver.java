package com.asa.lab.internalimp.datasource.empty;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriver;
import com.asa.lab.internalimp.sql.rdd.BaseDSIterator;
import com.asa.lab.internalimp.sql.rdd.BasePartition;
import com.asa.lab.internalimp.sql.rdd.ComputeOption;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.RowSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/12.
 */
public class EmptyDataSourceDriver implements DataSourceDriver {

    @Override
    public BasePartition[] getPartitions(DataSource source) {

        return new BasePartition[]{new BasePartition()};
    }

    @Override
    public BaseDSIterator compute(DataSource source, ComputeOption option) {

        List<RowSet> rowSets = new ArrayList<>();
        Iterator<RowSet> iterator = rowSets.iterator();
        return new BaseDSIterator(source, iterator, option);
    }
}
