package com.asa.lab.internalimp.sql;

import com.asa.lab.internalimp.sql.relation.BaseDSRelation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.immutable.Map;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class DSRelationProvider implements CreatableRelationProvider, RelationProvider, DataSourceRegister {

    private static final String DSDB = "DSDB";

    public final static String CLASSPATH = DSRelationProvider.class.getName();

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {

        DSRelationOptions options = new DSRelationOptions(parameters);
        BaseDSRelation dsRelation = new BaseDSRelation(sqlContext, options);
        return dsRelation;
    }

    @Override
    public String shortName() {

        return DSDB;
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {

        return null;
    }
}
