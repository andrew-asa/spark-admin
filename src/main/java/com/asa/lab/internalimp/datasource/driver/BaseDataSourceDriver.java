package com.asa.lab.internalimp.datasource.driver;

import com.asa.lab.internalimp.sql.rdd.BaseDSIterator;
import com.asa.lab.internalimp.sql.rdd.BasePartition;
import com.asa.lab.internalimp.sql.rdd.ComputeOption;
import com.asa.lab.structure.datasource.DataSource;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 */
public class BaseDataSourceDriver implements DataSourceDriver {

    @Override
    public BasePartition[] getPartitions(DataSource source) {

        return new BasePartition[]{new BasePartition()};
    }

    @Override
    public BaseDSIterator compute(DataSource source, ComputeOption option) {

        return new BaseDSIterator(source, option);
    }
}
