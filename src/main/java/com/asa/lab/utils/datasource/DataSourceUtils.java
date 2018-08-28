package com.asa.lab.utils.datasource;

import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasourceBuilder;
import com.asa.lab.structure.resultset.Type;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public class DataSourceUtils {

    /**
     * 模拟一个data source
     *
     * @param types
     * @param data
     * @return
     */
    public static MemoryDatasource mockMemoryDatasource(Type[] types, Object[][] data) {

        MemoryDatasourceBuilder builder = new MemoryDatasourceBuilder();
        return builder
                .setData(data)
                .setTypes(types)
                .build();
    }
}
