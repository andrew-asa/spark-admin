package com.asa.lab.internalimp.sql.relation;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.internalimp.sql.rdd.DataSourceRDD;
import com.asa.lab.structure.datasource.DataSource;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.StructType;

/**
 * @author andrew_asa
 * @date 2018/11/6.
 */
public class BaseDataSourceRelation extends BaseRelation implements PrunedFilteredScan, InsertableRelation {

    private DataSource dataSource;

    private SQLContext sqlContext;

    public BaseDataSourceRelation(DataSource dataSource, SQLContext sqlContext) {

        this.dataSource = dataSource;
        this.sqlContext = sqlContext;
    }

    @Override
    public void insert(Dataset<Row> data, boolean overwrite) {

        System.out.println();
    }

    @Override
    public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {

        return (RDD) (new DataSourceRDD(dataSource));
    }

    @Override
    public SQLContext sqlContext() {

        return sqlContext;
    }

    @Override
    public StructType schema() {

        return DataSourceDriverContent.getInstance().buildSchema(dataSource);
    }
}
