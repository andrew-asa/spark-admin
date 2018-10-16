package com.asa.lab.structure.service.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

/**
 * @author andrew_asa
 * @date 2018/10/16.
 */
public class SparkFunctionsHelper {

    /**
     * 获取季节
     *
     * @param columnName
     * @param dataset
     * @return
     */
    public static Column season(String columnName, Dataset dataset) {

        return functions.ceil(functions.month(dataset.col(columnName)).divide(3));
    }
}
