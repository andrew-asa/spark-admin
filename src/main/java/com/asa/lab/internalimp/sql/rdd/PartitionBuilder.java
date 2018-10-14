package com.asa.lab.internalimp.sql.rdd;

import com.asa.lab.structure.datasource.DataSource;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.Iterator;

/**
 * @author andrew_asa
 * @date 2018/10/11.
 */
public interface PartitionBuilder {

    Iterator<InternalRow> compute(DataSource source, Partition split, TaskContext context);

    Partition[] getPartitions(DataSource source);
}
