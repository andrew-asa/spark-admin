package com.asa.lab.internalimp.sql.rdd;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.RowSet;
import com.asa.utils.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Map;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/12.
 */
public class BaseInternalRow implements Row {

    private RowSet rowSet;

    private DataSource dataSource;

    private ComputeOption option;

    /**
     * 需要的列
     */
    private String[] requiredColumns;

    public BaseInternalRow(DataSource dataSource, RowSet rowSet, ComputeOption option) {

        this.dataSource = dataSource;
        this.rowSet = rowSet;
        this.option = option;
        this.requiredColumns = option.getRequiredColumns();
    }

    public RowSet getRowSet() {

        return rowSet;
    }

    public void setRowSet(RowSet rowSet) {

        this.rowSet = rowSet;
    }

    @Override
    public int size() {

        return rowSet.size();
    }

    @Override
    public int length() {

        return rowSet.size();
    }

    @Override
    public StructType schema() {

        return DataSourceDriverContent.getInstance().buildSchema(dataSource);
    }

    @Override
    public Object apply(int i) {

        if (requiredColumns.length > 0) {

            String columun = requiredColumns[i];
            int index = fieldIndex(columun);
            if (index >= 0) {
                return rowSet.apply(index);
            }
        }
        return rowSet.apply(i);
    }

    @Override
    public Object get(int i) {

        return rowSet.apply(i);
    }

    @Override
    public boolean isNullAt(int i) {

        return rowSet.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {

        return rowSet.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {

        return rowSet.getByte(i);
    }

    @Override
    public short getShort(int i) {

        return rowSet.getShort(i);
    }

    @Override
    public int getInt(int i) {

        return rowSet.getInt(i);
    }

    @Override
    public long getLong(int i) {

        return rowSet.getLong(i);
    }

    @Override
    public float getFloat(int i) {

        return rowSet.getFloat(i);
    }

    @Override
    public double getDouble(int i) {

        return rowSet.getDouble(i);
    }

    @Override
    public String getString(int i) {

        return rowSet.getString(i);
    }


    @Override
    public BigDecimal getDecimal(int i) {

        return rowSet.getDecimal(i);
    }

    @Override
    public Date getDate(int i) {

        return rowSet.getDate(i);
    }

    @Override
    public Timestamp getTimestamp(int i) {

        return null;
    }

    @Override
    public <T> Seq<T> getSeq(int i) {

        return null;
    }

    @Override
    public <T> List<T> getList(int i) {

        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(int i) {

        return null;
    }


    @Override
    public <K, V> java.util.Map<K, V> getJavaMap(int i) {

        return null;
    }

    @Override
    public Row getStruct(int i) {

        return null;
    }

    @Override
    public <T> T getAs(int i) {

        return null;
    }

    @Override
    public <T> T getAs(String fieldName) {

        return null;
    }

    @Override
    public int fieldIndex(String name) {

        Column[] columns = dataSource.getSchema().getColumns();
        for (int i = 0; i < columns.length; i++) {
            if (StringUtils.equals(columns[i].getName(), name)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> fieldNames) {

        return null;
    }

    @Override
    public Row copy() {

        return null;
    }


    @Override
    public boolean anyNull() {

        return false;
    }

    @Override
    public Seq<Object> toSeq() {

        List<Object> obs = ArrayUtils.arrayToList(rowSet.toArrary());
        return JavaConverters.asScalaBufferConverter(obs).asScala().toSeq();
    }

    @Override
    public String mkString() {

        return null;
    }

    @Override
    public String mkString(String sep) {

        return null;
    }

    @Override
    public String mkString(String start, String sep, String end) {

        return null;
    }
}
