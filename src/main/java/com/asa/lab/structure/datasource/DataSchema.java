package com.asa.lab.structure.datasource;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 * 数据元信息
 */
public interface DataSchema {

    Column[] getColumns();

    void setColumns(Column[] columns);
}
