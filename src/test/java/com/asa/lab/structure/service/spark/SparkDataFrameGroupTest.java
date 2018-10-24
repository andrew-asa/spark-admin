package com.asa.lab.structure.service.spark;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.structure.datasource.Type;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author andrew_asa
 * @date 2018/10/24.
 */
public class SparkDataFrameGroupTest {

    private MemoryDatasource dataSource1;

    private MemoryDatasource dataSource2;

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
        dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "sum"},
                new Object[][]{
                        {"a", 1.0},
                        {"b", 2.0},
                        {"b", 2.0},
                        {"c", 3.0},
                        {"d", 4.0},
                        {"d", 5.0},
                });
        dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String,Type.String, Type.Double},
                new String[]{"column1","column2", "sum"},
                new Object[][]{
                        {"a","a_1", 1.0},
                        {"a","a_1", 1.0},
                        {"a","a_2", 1.0},
                        {"a","a_2", 1.0},
                        {"b","b_1", 1.0},
                        {"b","b_2", 1.0},
                        {"a","a_2", 1.0},

                });
        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testGroup_sum() {


        //DataSourceHelper.show(dataSource1);
        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> ret = dataSet.groupBy("column").sum("sum");
        ret.show();
    }

    @Test
    public void testGroup_sum2() {


        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource2.getName());
        Dataset<Row> ret = dataSet.groupBy("column1","column2").sum("sum");
        ret.show();
        DataSourceHelper.show(dataSource2);
    }

    @Test
    public void testGroup_max() {


        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> ret = dataSet.groupBy("column").max("sum");
        ret.show();
        DataSourceHelper.show(dataSource1);
    }

    @Test
    public void testGroup_min() {

        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> ret = dataSet.groupBy("column").min("sum");
        ret.show();
        DataSourceHelper.show(dataSource1);
    }

    @Test
    public void testGroup_avg() {

        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> ret = dataSet.groupBy("column").avg("sum");
        ret.show();
        ret = dataSet.groupBy("column").count();
        ret.show();
    }

    @Test
    public void testGroup_count() {


        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> ret = dataSet.groupBy("column").count();
        ret.show();
        DataSourceHelper.show(dataSource1);
    }

    @Test
    public void testGroup_agg_sum() {

        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> ret = dataSet.groupBy("column").agg(functions.max(dataSet.col("sum")).as("sum1"));
        ret.show();
        DataSourceHelper.show(dataSource1);
    }

    @Test
    public void testGroup_agg_sum2() {

        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource2.getName());
        Dataset<Row> ret = dataSet.groupBy("column1","column2").agg(functions.max(dataSet.col("sum")).as("sum1"));
        ret.show();
        DataSourceHelper.show(dataSource2);
    }

}
