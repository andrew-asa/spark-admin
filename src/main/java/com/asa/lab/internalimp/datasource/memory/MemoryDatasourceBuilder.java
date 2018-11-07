package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.internalimp.datasource.BaseColumn;
import com.asa.lab.internalimp.datasource.BaseDataSchema;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.Type;
import com.asa.utils.ArrayUtils;
import com.asa.utils.StringUtils;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryDatasourceBuilder {

    private Type[] types;

    private String[] names;

    private Object[][] data;

    private String tableName;

    public MemoryDatasourceBuilder setColumn(Type[] types, String[] names) {

        this.types = types;
        this.names = names;
        return this;
    }

    public MemoryDatasourceBuilder setData(Object[][] data) {

        this.data = data;
        return this;
    }

    public MemoryDatasourceBuilder setTableName(String tableName) {

        this.tableName = tableName;
        return this;
    }

    public MemoryDatasource build() {

        if (ArrayUtils.length(types) != ArrayUtils.length(names) && ArrayUtils.length(types) != ArrayUtils.length(data)) {
            throw new RuntimeException(
                    StringUtils.messageFormat(
                            "type length {} , names length {} , data length {} no the same",
                            ArrayUtils.length(types),
                            ArrayUtils.length(names),
                            ArrayUtils.length(data)));
        }

        MemoryDatasource datasource = new MemoryDatasource();


        int columnLen = types.length;

        Column[] columns = new Column[columnLen];
        for (int i = 0; i < columnLen; i++) {
            BaseColumn column = new BaseColumn(types[i], names[i]);
            columns[i] = column;
        }

        BaseDataSchema dataSchema = new BaseDataSchema(columns);
        datasource.setSchema(dataSchema);
        MemoryRowSet[] rows;
        if (data != null) {
            rows = new MemoryRowSet[data.length];
            for (int i = 0; i < data.length; i++) {
                MemoryRowSet row = new MemoryRowSet(columns, data[i]);
                row.setColumns(columns);
                rows[i] = row;
            }
        } else {
            rows = new MemoryRowSet[0];
        }
        MemoryDataSet set = new MemoryDataSet(rows, dataSchema);
        datasource.setDataSet(set);
        datasource.setName(tableName);
        clear();
        return datasource;
    }

    private void clear() {

        types = null;
        data = null;
    }
}
