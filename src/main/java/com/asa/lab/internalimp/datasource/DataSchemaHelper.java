package com.asa.lab.internalimp.datasource;

import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.Type;
import com.asa.utils.ArrayUtils;
import com.asa.utils.ComparatorUtils;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 */
public class DataSchemaHelper {

    private DataSchema schema;

    public DataSchemaHelper(DataSchema schema) {

        this.schema = schema;
    }

    public DataSchema getSchema() {

        return schema;
    }

    public void setSchema(DataSchema schema) {

        this.schema = schema;
    }

    /**
     * 获取索引
     *
     * @param columnName
     * @return
     */
    public int getColumnIndex(String columnName) {

        Column[] columns = schema.getColumns();
        int len = ArrayUtils.length(columns);
        for (int i = 0; i < len; i++) {
            if (ComparatorUtils.equals(columns[i].getName(), columnName)) {
                return i;
            }
        }
        return -1;
    }

    public Column getColumn(String columnName) {

        Column[] columns = schema.getColumns();
        int len = ArrayUtils.length(columns);
        for (int i = 0; i < len; i++) {
            if (ComparatorUtils.equals(columns[i].getName(), columnName)) {
                return columns[i];
            }
        }
        return null;
    }

    public Type getColumnType(String columnName) {

        Column[] columns = schema.getColumns();
        int len = ArrayUtils.length(columns);
        for (int i = 0; i < len; i++) {
            if (ComparatorUtils.equals(columns[i].getName(), columnName)) {
                return columns[i].getType();
            }
        }
        return null;
    }
}
