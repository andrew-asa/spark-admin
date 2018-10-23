package com.asa.lab.structure.service.spark;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.datasource.spark.SparkDataSource;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;

import static org.junit.Assert.*;

/**
 * @author andrew_asa
 * @date 2018/10/16.
 */
public class SparkFunctionsHelperTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testSeason() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.Date},
                new String[]{"dead"},
                new Object[][]{
                        {Date.valueOf("2069-01-01")},
                        {Date.valueOf("2069-02-02")},
                        {Date.valueOf("2069-03-03")},
                        {Date.valueOf("2069-04-04")},
                        {Date.valueOf("2069-05-05")},
                        {Date.valueOf("2069-06-06")},
                        {Date.valueOf("2069-07-07")},
                        {Date.valueOf("2069-08-08")},
                        {Date.valueOf("2069-09-09")},
                        {Date.valueOf("2069-10-10")},
                        {Date.valueOf("2069-11-11")},
                        {Date.valueOf("2069-12-12")},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);
        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource.getName());
        dataSet = dataSet.withColumn("newColumn", SparkFunctionsHelper.season("dead", dataSet));
        dataSet.show();
        DataSource result = new SparkDataSource(dataSet);

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.Date, Type.Long},
                new String[]{"dead", "newColumn"},
                new Object[][]{
                        {Date.valueOf("2069-01-01"), 1L},
                        {Date.valueOf("2069-02-02"), 1L},
                        {Date.valueOf("2069-03-03"), 1L},
                        {Date.valueOf("2069-04-04"), 2L},
                        {Date.valueOf("2069-05-05"), 2L},
                        {Date.valueOf("2069-06-06"), 2L},
                        {Date.valueOf("2069-07-07"), 3L},
                        {Date.valueOf("2069-08-08"), 3L},
                        {Date.valueOf("2069-09-09"), 3L},
                        {Date.valueOf("2069-10-10"), 4L},
                        {Date.valueOf("2069-11-11"), 4L},
                        {Date.valueOf("2069-12-12"), 4L},
                });
        Assert.assertTrue(DataSourceHelper.equalDataSet(result, expect));
    }

    @Test
    public void testUnion() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"table1_column_row1", 3.0},
                        {"table1_column_row2", 4.0},
                        {"table1_column_row3", 5.0},
                        {"table1_column_row4", 6.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table2_column", "table2_width"},
                new Object[][]{
                        {"table2_column_row1", 7.0},
                        {"table2_column_row2", 8.0},
                        {"table2_column_row3", 9.0},
                        {"table2_column_row4", 10.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        MemoryDatasource expect1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"table1_column_row1", 3.0},
                        {"table1_column_row2", 4.0},
                        {"table1_column_row3", 5.0},
                        {"table1_column_row4", 6.0},
                        {"table2_column_row1", 7.0},
                        {"table2_column_row2", 8.0},
                        {"table2_column_row3", 9.0},
                        {"table2_column_row4", 10.0},
                });
        Dataset<Row> union1 = dataFrame1.union(dataFrame2);
        DataSource unionDataSource1 = new SparkDataSource(union1);
        Assert.assertTrue(DataSourceHelper.assertDataSource(unionDataSource1, expect1));

        Dataset<Row> dataFrame11 = dataFrame1.select(new Column[]{
                dataFrame1.col("table1_column"),
                dataFrame1.col("table1_width"),
                functions.lit(null).as("table2_column").cast(DataTypes.StringType),
                functions.lit(null).as("table2_width").cast(DataTypes.DoubleType)
        });

        Dataset<Row> dataFrame22 = dataFrame2.select(
                functions.lit(null).as("table1_column").cast(DataTypes.StringType),
                functions.lit(null).as("table1_width").cast(DataTypes.DoubleType),
                dataFrame2.col("table2_column"),
                dataFrame2.col("table2_width"));

        MemoryDatasource expect2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double, Type.String, Type.Double},
                new String[]{"table1_column", "table1_width", "table2_column", "table2_width"},
                new Object[][]{
                        {"table1_column_row1", 3.0, null, null},
                        {"table1_column_row2", 4.0, null, null},
                        {"table1_column_row3", 5.0, null, null},
                        {"table1_column_row4", 6.0, null, null},
                        {null, null, "table2_column_row1", 7.0},
                        {null, null, "table2_column_row2", 8.0},
                        {null, null, "table2_column_row3", 9.0},
                        {null, null, "table2_column_row4", 10.0},
                });

        Dataset<Row> union2 = dataFrame11.union(dataFrame22);
        DataSource unionDataSource2 = new SparkDataSource(union2);
        Assert.assertTrue(DataSourceHelper.assertDataSource(unionDataSource2, expect2));
    }
}