package com.asa.lab.internalimp.datasource.driver;

import com.asa.lab.internalimp.datasource.empty.EmptyDataSource;
import com.asa.lab.internalimp.datasource.empty.EmptyDataSourceDriver;
import com.asa.lab.internalimp.datasource.memory.MemoryDataSourceDriver;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.datasource.relation.RelationTablesDataSource;
import com.asa.lab.internalimp.sql.relation.ColumnTypeToDataTypeVisitor;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/11.
 */
public class DataSourceDriverContent {

    private static DataSourceDriverContent INSTANCE;

    private Map<String, DataSourceDriver> dataSourceDriverMap;

    private DataSourceDriverContent() {

        init();
    }

    private void init() {

        dataSourceDriverMap = new HashMap<String, DataSourceDriver>();
    }

    public void registerDefaultDriver() {

        registerDriver(MemoryDatasource.URLSCHMA, new MemoryDataSourceDriver());
        registerDriver(EmptyDataSource.URLSCHMA, new EmptyDataSourceDriver());
        registerDriver(RelationTablesDataSource.URLSCHMA, new BaseDataSourceDriver());
    }

    public static DataSourceDriverContent getInstance() {

        if (INSTANCE == null) {
            synchronized (DataSourceDriverContent.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DataSourceDriverContent();
                }
            }
        }
        return INSTANCE;
    }

    public DataSourceDriver getDriver(DataSource source) {

        return dataSourceDriverMap.get(source.getURIScheme());
    }

    public void registerDriver(String uriScheme, DataSourceDriver driver) {

        MapUtils.safeAddToMap(dataSourceDriverMap, uriScheme, driver);
    }

    public StructType buildSchema(DataSource source) {

        List<StructField> structFields = getStructFields(source);
        return new StructType(structFields.toArray(new StructField[0]));
    }

    public List<StructField> getStructFields(DataSource source) {

        List<StructField> ret = new ArrayList<>();
        DataSchema schema = source.getSchema();
        Column[] columns = schema.getColumns();
        for (Column column : columns) {
            Type type = column.getType();
            String name = column.getName();
            DataType dataType = convertToDataType(type);
            ret.add(new StructField(name, dataType, true, Metadata.empty()));
        }
        return ret;
    }

    public static DataType convertToDataType(Type type) {

        return type.accept(ColumnTypeToDataTypeVisitor.INSTANT);
    }
}
