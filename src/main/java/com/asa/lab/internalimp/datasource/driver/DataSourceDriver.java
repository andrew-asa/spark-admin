package com.asa.lab.internalimp.datasource.driver;

import com.asa.lab.internalimp.sql.rdd.BaseDSIterator;
import com.asa.lab.internalimp.sql.rdd.BasePartition;
import com.asa.lab.internalimp.sql.rdd.ComputeOption;
import com.asa.lab.structure.datasource.DataSource;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public interface DataSourceDriver {

    BasePartition[] getPartitions(DataSource source);

    BaseDSIterator compute(DataSource source,ComputeOption option);
}
