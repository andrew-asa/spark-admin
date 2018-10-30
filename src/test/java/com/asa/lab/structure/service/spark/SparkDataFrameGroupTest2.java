package com.asa.lab.structure.service.spark;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.structure.datasource.Type;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/30.
 */
public class SparkDataFrameGroupTest2 {


    private MemoryDatasource dataSource;

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
        dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double},
                new String[]{"column1", "column2", "sum"},
                new Object[][]{
                        {"a", "a_1", 1.0},
                        {"a", "a_1", 1.0},
                        {"a", "a_2", 1.0},
                        {"a", "a_2", 1.0},
                        {"b", "b_1", 1.0},
                        {"b", "b_2", 1.0},
                        {"a", "a_2", 1.0},

                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testGroup_agg_sum4() throws AnalysisException {

        Dataset<Row> dataSet = SparkContentManager.getInstance().getDataset(dataSource.getName());
        Dataset<Row> ret;
        dataSet.createTempView("temp");
        ret = dataSet.sparkSession().sql("select `column1`, sum(`sum`)\n" +
                                                 "from `temp`\n" +
                                                 "group by `column1`\n");
        ret.show();
        DataSourceHelper.show(dataSource);
    }
}
