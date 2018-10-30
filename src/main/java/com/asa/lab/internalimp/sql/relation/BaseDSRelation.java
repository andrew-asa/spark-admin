package com.asa.lab.internalimp.sql.relation;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.internalimp.sql.DSConstant;
import com.asa.lab.internalimp.sql.DSRelationOptions;
import com.asa.lab.internalimp.sql.rdd.DSRDD;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.fasterxml.jackson.databind.DatabindContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 * 分布式表基本relations
 */
public class BaseDSRelation extends BaseRelation implements PrunedFilteredScan, InsertableRelation {


    private SQLContext sqlContext;

    private DSRelationOptions options;

    public BaseDSRelation(SQLContext sqlContext, DSRelationOptions options) {

        this.sqlContext = sqlContext;
        this.options = options;
    }

    @Override
    public void insert(Dataset<Row> data, boolean overwrite) {

        System.out.println();
    }

    @Override
    public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {

        return (RDD) (new DSRDD(getTableName(), options, requiredColumns, filters));
    }

    @Override
    public SQLContext sqlContext() {

        return sqlContext;
    }

    @Override
    public StructType schema() {

        String tableName = getTableName();
        DataSource source = DataBaseContent.getInstance().getDataSource(tableName);
        return DataSourceDriverContent.getInstance().buildSchema(source);
    }

    private String getTableName() {

        return options.getValue(DSConstant.TABLE_NAME);
    }
}
