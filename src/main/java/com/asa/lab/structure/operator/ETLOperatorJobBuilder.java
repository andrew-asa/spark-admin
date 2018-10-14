package com.asa.lab.structure.operator;

import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author andrew_asa
 * @date 2018/10/14.
 * etl 动作执行器
 */
public interface ETLOperatorJobBuilder {

    Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content);
}
