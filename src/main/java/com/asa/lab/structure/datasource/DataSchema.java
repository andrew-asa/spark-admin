package com.asa.lab.structure.datasource;

import java.io.Serializable;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 * 数据元信息
 */
public interface DataSchema extends Serializable {

    Column[] getColumns();

    void setColumns(Column[] columns);
}
