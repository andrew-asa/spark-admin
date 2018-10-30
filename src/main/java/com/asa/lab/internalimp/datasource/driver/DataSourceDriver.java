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

    /**
     * 获取分区信息
     *
     * @param source
     * @return
     */
    BasePartition[] getPartitions(DataSource source);

    /**
     * 生成结果迭代器
     *
     * @param source
     * @param option
     * @return
     */
    BaseDSIterator compute(DataSource source, ComputeOption option);
}
