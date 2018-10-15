package com.asa.lab.internalimp.datasource;

import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasourceBuilder;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.utils.ArrayUtils;
import com.asa.lab.utils.ComparatorUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class DataSourceHelper {

    private static final int DEFAULT_DATASOURCE_NAME_LEN = 6;

    /**
     * 模拟一个data source
     *
     * @param tableName
     * @param types
     * @param data
     * @return
     */
    public static MemoryDatasource mockMemoryDatasource(String tableName, Type[] types, String[] names, Object[][] data) {

        MemoryDatasourceBuilder builder = new MemoryDatasourceBuilder();
        return builder
                .setTableName(tableName)
                .setData(data)
                .setColumn(types, names)
                .build();
    }

    public static MemoryDatasource mockMemoryDatasource(Type[] types, String[] names, Object[][] data) {

        return mockMemoryDatasource(createDataSourceName(), types, names, data);
    }

    public static MemoryDatasource emptyMemoryDataSource() {

        return mockMemoryDatasource(createDataSourceName(), new Type[0], new String[0], new Object[0][0]);
    }

    public static void makeSureDataSourceHaveName(DataSource dataSource) {

        if (dataSource != null) {
            String name = dataSource.getName();
            if (StringUtils.isEmpty(name)) {
                dataSource.setName(createDataSourceName());
            }
        }
    }

    public static String createDataSourceName() {

        return RandomStringUtils.randomAlphanumeric(DEFAULT_DATASOURCE_NAME_LEN);
    }

    /**
     * 数据是否相同
     *
     * @param ds1
     * @param ds2
     * @return
     */
    public static boolean equalDataSet(DataSource ds1, DataSource ds2) {

        if ((ds1 != null && ds2 == null) || (ds1 == null && ds2 != null)) {
            return false;
        }
        if (ds1 != null && ds2 != null) {

            DataSchema dssc1 = ds1.getSchema();
            DataSchema dssc2 = ds2.getSchema();
            int cs = ArrayUtils.length(dssc1.getColumns());
            if (cs != ArrayUtils.length(dssc2.getColumns())) {
                return false;
            }
            DataSet dss1 = ds1.getDataSet();
            int rs = dss1.size();
            DataSet dss2 = ds2.getDataSet();
            if (rs != dss2.size()) {
                return false;
            }
            for (int i = 0; i < rs; i++) {
                for (int j = 0; j < cs; j++) {
                    if (!ComparatorUtils.equals(dss1.getObject(i, j), dss2.getObject(i, j))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static void show(DataSource source) {

        show(source, 200);
    }

    public static void show(DataSource source, int size) {

        StringBuffer ret = new StringBuffer();
        if (source != null) {
            DataSchema schema = source.getSchema();
            Column[] columns = schema.getColumns();
            if (ArrayUtils.length(columns) > 0) {
                StringBuffer head = new StringBuffer();
                StringBuffer sp = new StringBuffer();
                head.append("|");
                sp.append("+");
                for (Column column : columns) {
                    String cn = column.getName();
                    int len = cn.length() / "-".length();
                    sp.append(com.asa.lab.utils.StringUtils.clone("-", len));
                    head.append(cn);
                    head.append("|");
                    sp.append("+");
                }
                sp.append("\n");
                head.append("\n");
                String spStr = sp.toString();
                ret.append(spStr).append(head.toString());
                DataSet set = source.getDataSet();
                int ps = Math.min(size, set.size());
                StringBuffer row = null;
                for (int i = 0; i < ps; i++) {
                    row = new StringBuffer();
                    row.append("|");
                    for (int j = 0; j < columns.length; j++) {
                        row.append(set.getObject(i, j));
                        row.append("|");
                    }
                    row.append("\n");
                    ret.append(row.toString());
                }
                ret.append(spStr);
            }
        }
        System.out.println(ret.toString());
    }

    public static DataSource translateToDataSource(Dataset<Row> dataset) {

        return null;
    }
}
