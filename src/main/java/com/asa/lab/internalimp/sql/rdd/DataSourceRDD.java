package com.asa.lab.internalimp.sql.rdd;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriver;
import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSource;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.Iterator;

/**
 * @author andrew_asa
 * @date 2018/11/6.
 */
public class DataSourceRDD extends BaseRDD<InternalRow> {

    private DataSource source;

    public DataSourceRDD(DataSource source) {

        super(InternalRow.class);
        this.source = source;
    }

    @Override
    public Iterator<InternalRow> compute(Partition split, TaskContext context) {

        DataSchema schema = source.getSchema();
        Column[] columns = schema.getColumns();
        String[] requiredColumns = new String[columns.length];
        for (int i = 0, len = columns.length; i < len; i++) {
            requiredColumns[i] = columns[i].getName();
        }
        ComputeOption option = new ComputeOption(requiredColumns);
        return (Iterator) getDataSourceDriver().compute(source, option);
    }

    @Override
    public Partition[] getPartitions() {

        return getDataSourceDriver().getPartitions(source);
    }

    private DataSource getDataSource() {

        return source;
    }

    private DataSourceDriver getDataSourceDriver() {

        return DataSourceDriverContent.getInstance().getDriver(getDataSource());
    }
}