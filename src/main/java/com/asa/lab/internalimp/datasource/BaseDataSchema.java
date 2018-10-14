package com.asa.lab.internalimp.datasource;

import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class BaseDataSchema implements DataSchema {

    private Column[] columns;

    public BaseDataSchema(Column[] columns) {

        this.columns = columns;
    }

    @Override
    public Column[] getColumns() {

        return columns;
    }

    @Override
    public void setColumns(Column[] columns) {

        this.columns = columns;
    }
}
