package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.internalimp.datasource.BaseColumn;
import com.asa.lab.internalimp.datasource.BaseDataSchema;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.Type;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryDatasourceBuilder {

    private Type[] types;

    private String[] names;

    private Object[][] data;

    public MemoryDatasourceBuilder setColumn(Type[] types, String[] names) {

        this.types = types;
        this.names = names;
        return this;
    }

    public MemoryDatasourceBuilder setData(Object[][] data) {

        this.data = data;
        return this;
    }

    public MemoryDatasource build() {

        MemoryDatasource datasource = new MemoryDatasource();
        MemoryDataSet set = new MemoryDataSet();
        int columnLen = types.length;
        Column[] columns = new Column[columnLen];
        for (int i = 0; i < columnLen; i++) {
            BaseColumn column = new BaseColumn(types[i], names[i]);
            columns[i] = column;
        }
        BaseDataSchema dataSchema = new BaseDataSchema(columns);
        datasource.setSchema(dataSchema);
        if (data != null) {
            MemoryRowSet[] rows = new MemoryRowSet[data.length];
            for (int i = 0; i < data.length; i++) {
                MemoryRowSet row = new MemoryRowSet(columns, data[i]);
                rows[i] = row;
            }
            set.setData(rows);
        }
        datasource.setDataSet(set);
        clear();
        return datasource;
    }

    private void clear() {

        types = null;
        data = null;
    }
}
