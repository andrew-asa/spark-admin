package com.asa.lab.internalimp.sql.datasource;

import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.empty.EmptyDataSource;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.datasource.spark.SparkDataSource;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.RowSet;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.service.spark.SparkContentManager;
import com.asa.utils.ArrayUtils;
import com.asa.utils.ListUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/11/5.
 */
public class DataSourceDataSetBuilderTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testBuild() throws Exception {

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.Double, Type.String, Type.String, Type.Double},
                new String[]{"table1_width", "table2_column1", "table2_column2", "table2_width"},
                new Object[][]{
                        {3.0, "row_a", "table2_column2_2a", 7.0},
                        {3.0, "row_a", "table2_column2_2a", 8.0},
                        {4.0, "row_b", "table2_column2_2b", 9.0},
                        {4.0, "row_b", "table2_column2_2b", 10.0},
                        {5.0, "row_c", "table2_column2_2c", 11.0},
                        {5.0, "row_c", "table2_column2_2c", 12.0},
                });


        DataSourceDataSetBuilder builder = new DataSourceDataSetBuilder();
        Dataset<Row> dataset = builder.build(expect);
        SparkDataSource sparkDataSource = new SparkDataSource(dataset);
        Assert.assertTrue(DataSourceHelper.assertDataSource(sparkDataSource, expect));
    }

    @Test
    public void testEmptyDataSource() {

        DataSourceDataSetBuilder dataSetBuilder = new DataSourceDataSetBuilder();
        Dataset<Row> dataSet = dataSetBuilder.build(new EmptyDataSource());
        dataSet.printSchema();
        dataSet.show();
        SparkDataSource dataSource = new SparkDataSource(dataSet);
        DataSet set = dataSource.getDataSet();
        List<RowSet> rowSets = set.getDataList();
        Assert.assertEquals(ListUtils.length(rowSets), 0);
        DataSchema schema = dataSource.getSchema();
        Column[] columns = schema.getColumns();
        Assert.assertEquals(ArrayUtils.length(columns), 0);
    }

    @Test
    public void testMakeRdd() {

        SparkContext context = SparkContentManager.getInstance().getSession().sparkContext();
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList("a", "b", "b", "d").iterator()).asScala().toSeq();
        RDD<String> rdd = context.parallelize(seq, context.defaultParallelism(), ClassTag$.MODULE$.apply(String.class));
    }

    @Test
    public void testMakeDataSet() {

    }
}