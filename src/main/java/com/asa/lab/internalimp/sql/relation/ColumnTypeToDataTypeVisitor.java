package com.asa.lab.internalimp.sql.relation;

import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.datasource.TypeVisitor;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

/**
 * @author andrew_asa
 * @date 2018/10/11.
 */
public class ColumnTypeToDataTypeVisitor implements TypeVisitor<DataType> {

    public static ColumnTypeToDataTypeVisitor INSTANT = new ColumnTypeToDataTypeVisitor();

    public static Type convertToDataType(StructField structField) {
        DataType dataType = structField.dataType();
        if (DataTypes.BooleanType.equals(dataType)) {
            return Type.Bool;
        } else if (DataTypes.IntegerType.equals(dataType)) {
            return Type.Int;
        } else if (DataTypes.LongType.equals(dataType)) {
            return Type.Long;
        } else if (DataTypes.FloatType.equals(dataType)) {
            return Type.Float;
        } else if (DataTypes.DoubleType.equals(dataType)) {
            return Type.Double;
        } else if (DataTypes.StringType.equals(dataType)) {
            return Type.String;
        } else if (DataTypes.DateType.equals(dataType)) {
            return Type.Date;
        } else if (DataTypes.TimestampType.equals(dataType)) {
            if (structField.metadata().contains("time") && structField.metadata().getBoolean("time")) {
                return Type.Time;
            } else {
                return Type.Timestamp;
            }
        }
        throw new RuntimeException();
    }

    @Override
    public DataType visitBool() {

        return BooleanType;
    }

    @Override
    public DataType visitInt() {

        return IntegerType;
    }

    @Override
    public DataType visitFloat() {

        return FloatType;
    }

    @Override
    public DataType visitLong() {

        return LongType;
    }

    @Override
    public DataType visitDouble() {

        return DoubleType;
    }

    @Override
    public DataType visitString() {

        return StringType;
    }

    @Override
    public DataType visitDate() {

        return DateType;
    }

    @Override
    public DataType visitTime() {

        return TimestampType;
    }

    @Override
    public DataType visitTimestamp() {

        return TimestampType;
    }
}
