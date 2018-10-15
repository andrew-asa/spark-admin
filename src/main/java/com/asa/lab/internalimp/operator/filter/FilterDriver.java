package com.asa.lab.internalimp.operator.filter;

import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public interface FilterDriver {

    Dataset<Row> build(Dataset<Row> dataSet, FilterOperator operator, ETLJobBuilderContent content);
}
