package com.asa.lab.structure.service.spark;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.structure.datasource.Type;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author andrew_asa
 * @date 2018/10/19.
 */
public class SparkDataFrameJoinTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testJoin() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                        {"row_d", 6.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_b", 8.0},
                        {"row_c", 9.0},
                        {"row_d", 10.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2, "column");
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);
        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }

    @Test
    public void testJoin2() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_c", 8.0},
                        {"row_d", 9.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2, "column");
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);
        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }

    @Test
    public void testJoin3() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_a", 4.0},
                        {"row_d", 6.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_d", 9.0},
                        {"row_d", 10.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2, "column");
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);

        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }

    @Test
    public void testJoin4() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_a", 4.0},
                        {"row_d", 6.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table2_column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_d", 9.0},
                        {"row_d", 10.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2,
                                            dataFrame1.col("table1_column")
                                                    .equalTo(dataFrame2.col("table2_column")));
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);
        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }


    @Test
    public void testJoin_Outer() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_c", 8.0},
                        {"row_d", 9.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2,
                                            dataFrame1.col("column").equalTo(dataFrame2.col("column")),
                                            "outer");
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);
        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }

    @Test
    public void testJoin_Outer2() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_a", 4.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_a", 8.0},
                        {"row_c", 8.0},
                        {"row_d", 9.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2,
                                            dataFrame1.col("column").equalTo(dataFrame2.col("column")),
                                            "outer");
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);
        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }

    @Test
    public void testJoin_Outer3() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_a", 4.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table2_column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_a", 8.0},
                        {"row_c", 8.0},
                        {"row_d", 9.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2,
                                            dataFrame1.col("table1_column").equalTo(dataFrame2.col("table2_column")),
                                            "outer");
        join = join.select(join.col("table1_column").as("column"),
                           join.col("table1_width"),
                           join.col("table2_width"));
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);
        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }

    @Test
    public void testJoin_left() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_a", 4.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table2_column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_a", 8.0},
                        {"row_c", 8.0},
                        {"row_d", 9.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2,
                                            dataFrame1.col("table1_column").equalTo(dataFrame2.col("table2_column")),
                                            "left");
        join = join.select(join.col("table1_column").as("column"),
                           join.col("table1_width"),
                           join.col("table2_width"));
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);
        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }

    @Test
    public void testJoin_right() {

        MemoryDatasource dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_a", 4.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table2_column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_a", 8.0},
                        {"row_c", 8.0},
                        {"row_d", 9.0},
                });


        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);
        Dataset<Row> dataFrame1 = SparkContentManager.getInstance().getDataset(dataSource1.getName());
        Dataset<Row> dataFrame2 = SparkContentManager.getInstance().getDataset(dataSource2.getName());

        Dataset<Row> join = dataFrame1.join(dataFrame2,
                                            dataFrame1.col("table1_column").equalTo(dataFrame2.col("table2_column")),
                                            "right");
        join = join.select(join.col("table1_column").as("column"),
                           join.col("table1_width"),
                           join.col("table2_width"));
        join.show();
        DataSourceHelper.show(dataSource1);
        DataSourceHelper.show(dataSource2);
        //DataSource joinDataSource = new SparkDataSource(join);
        //Assert.assertTrue(DataSourceHelper.assertDataSource(joinDataSource, expect1));
    }
}
