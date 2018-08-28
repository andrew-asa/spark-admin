package com.asa.lab.internalimp.datasource.memory;

import com.asa.lab.structure.resultset.Type;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class MemoryDatasourceBuilder {

    private Type[] types;

    private Object[][] data;

    public MemoryDatasourceBuilder setTypes(Type[] types) {

        this.types = types;
        return this;
    }

    public MemoryDatasourceBuilder setData(Object[][] data) {

        this.data = data;
        return this;
    }

    public MemoryDatasource build() {

        MemoryDatasource datasource = new MemoryDatasource();
        MemoryDataSet set = new MemoryDataSet();
        set.setTypes(types);
        if (data != null) {
            MemoryRow[] rows = new MemoryRow[data.length];
            for (int i = 0; i < data.length; i++) {
                MemoryRow row = new MemoryRow();
                row.setData(data[i]);
            }
            set.setData(rows);
        }
        datasource.setSource(set);
        clear();
        return datasource;
    }

    private void clear() {

        types = null;
        data = null;
    }
}
