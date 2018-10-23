package com.asa.lab.internalimp.operator.join;


import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.etl.DefaultETLBuilder;
import com.asa.lab.internalimp.operator.select.SelectOperatorHelper;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/23.
 */
public class JoinTest {

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
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                        {"row_d", 6.0},
                });


        MemoryDatasource dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table2_column", "table2_width"},
                new Object[][]{
                        {"row_a", 7.0},
                        {"row_b", 8.0},
                        {"row_c", 9.0},
                        {"row_d", 10.0},
                });

        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);

        JoinOperatorBuilder joinOperatorBuilder = new JoinOperatorBuilder();
        JoinOperator joinOperator = joinOperatorBuilder.setRightTale(dataSource2.getName())
                .setJoinType(JoinType.inner)
                .setForceOrder(true)
                .addColumnItem("table1_column", "table2_column", "column")
                .build();
        DefaultETLBuilder builder2 = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators2 = new ArrayList<ETLOperator>();
        ETLOperators2.add(SelectOperatorHelper.allSelectOperator(dataSource1));
        ETLOperators2.add(joinOperator);
        ETLJobBuilderContent content2 = new ETLJobBuilderContent();
        DataSource result = builder2.build(content2, ETLOperators2);
        //DataSourceHelper.show(result2);

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double, Type.Double},
                new String[]{"column", "table1_width", "table2_width"},
                new Object[][]{
                        {"row_a", 3.0, 7.0},
                        {"row_b", 4.0, 8.0},
                        {"row_c", 5.0, 9.0},
                        {"row_d", 6.0, 10.0},
                });

        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

}