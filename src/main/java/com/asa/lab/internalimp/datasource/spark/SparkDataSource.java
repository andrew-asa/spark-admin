package com.asa.lab.internalimp.datasource.spark;

import com.asa.lab.internalimp.datasource.BaseColumn;
import com.asa.lab.internalimp.datasource.BaseDataSchema;
import com.asa.lab.internalimp.sql.relation.ColumnTypeToDataTypeVisitor;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/14.
 */
public class SparkDataSource implements DataSource {

    public static final String URLSCHMA = "spark-sql";

    private BaseDataSchema schema;

    private Dataset<Row> dataSet;

    public SparkDataSource(Dataset<Row> dataSet) {

        this.dataSet = dataSet;
        init();
    }

    private void init() {

        StructType structType = dataSet.schema();
        Iterator<StructField> iterator = structType.iterator();
        List<Column> columnList = new ArrayList<>();
        while (iterator.hasNext()) {
            StructField field = iterator.next();
            String name = field.name();
            Type type = ColumnTypeToDataTypeVisitor.convertToDataType(field);
            BaseColumn column = new BaseColumn(type, name);
            columnList.add(column);
        }
        schema = new BaseDataSchema(columnList.toArray(new Column[0]));
    }

    @Override
    public String getName() {

        return null;
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public DataSet getDataSet() {

        return new SparkDataSet(schema,dataSet);
    }

    @Override
    public DataSchema getSchema() {

        return schema;
    }

    @Override
    public void setSchema(DataSchema schema) {

        StructType structType = dataSet.schema();
        structType.toIterator();
    }

    @Override
    public String getURIScheme() {

        return URLSCHMA;
    }
}
