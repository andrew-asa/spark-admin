package com.asa.lab.internalimp.sql.rdd;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.driver.DataSourceDriver;
import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.internalimp.sql.DSRelationOptions;
import com.asa.lab.structure.datasource.DataSource;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import scala.collection.Iterator;

/**
 * @author andrew_asa
 * @date 2018/10/11.
 */
public class DSRDD extends BaseRDD<InternalRow> {

    private String tableName;

    private DSRelationOptions options;

    private String[] requiredColumns;

    private Filter[] filters;

    public DSRDD(String tableName, DSRelationOptions options, String[] requiredColumns, Filter[] filters) {

        super(InternalRow.class);
        this.tableName = tableName;
        this.options = options;
        this.requiredColumns = requiredColumns;
        this.filters = filters;
    }

    @Override
    public Iterator<InternalRow> compute(Partition split, TaskContext context) {

        ComputeOption option = new ComputeOption(split, context);
        DataSource source = getDataSource();
        return (Iterator)getDataSourceDriver().compute(source,option);
    }

    @Override
    public Partition[] getPartitions() {

        DataSource source = getDataSource();
        return getDataSourceDriver().getPartitions(source);
    }

    private DataSource getDataSource() {

        return DataBaseContent.getInstance().getDataSource(tableName);
    }

    private DataSourceDriver getDataSourceDriver() {

        return DataSourceDriverContent.getInstance().getDriver(getDataSource());
    }


}
