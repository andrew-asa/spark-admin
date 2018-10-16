package com.asa.lab.structure.service.spark;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.datasource.spark.SparkDataSource;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
}