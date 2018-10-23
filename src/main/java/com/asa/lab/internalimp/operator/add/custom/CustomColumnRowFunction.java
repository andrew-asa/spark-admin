package com.asa.lab.internalimp.operator.add.custom;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author andrew_asa
 * @date 2018/10/18.
 */
public class CustomColumnRowFunction implements MapPartitionsFunction<Row, Row>, Serializable {

    private AddCustomColumn operator;

    private StructType structType;

    public CustomColumnRowFunction(AddCustomColumn operator, StructType structType) {

        this.operator = operator;
        this.structType = structType;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) throws Exception {

        return new CustomColumnRowIterator(input, operator, structType);
    }
}
