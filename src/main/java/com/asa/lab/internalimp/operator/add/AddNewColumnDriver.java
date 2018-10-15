package com.asa.lab.internalimp.operator.add;

import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public interface AddNewColumnDriver {

    Dataset<Row> build(Dataset<Row> dataSet, AddNewColumnOperator operator, ETLJobBuilderContent content);
}
