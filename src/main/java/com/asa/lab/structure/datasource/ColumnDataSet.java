package com.asa.lab.structure.datasource;

import java.io.Serializable;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 * 单列数据
 */
public interface ColumnDataSet extends Serializable {

    /**
     * 第几行数据
     *
     * @param rowIndex
     * @return
     */
    Object getObject(int rowIndex);
}
