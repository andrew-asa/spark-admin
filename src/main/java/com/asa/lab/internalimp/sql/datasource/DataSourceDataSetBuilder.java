package com.asa.lab.internalimp.sql.datasource;

import com.asa.lab.internalimp.sql.relation.BaseDataSourceRelation;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.service.spark.SparkContentManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author andrew_asa
 * @date 2018/11/5.
 * dataSource转内存化RDD
 */
public class DataSourceDataSetBuilder {

    public Dataset<Row> build(DataSource dataSource) {

        SparkSession session = SparkContentManager.getInstance().getSession();
        BaseDataSourceRelation baseDataSourceRelation = new BaseDataSourceRelation(dataSource, session.sqlContext());
        return session.baseRelationToDataFrame(baseDataSourceRelation);
    }
}
