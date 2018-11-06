package com.asa.lab.structure.datasource;

import java.io.Serializable;
import java.util.List;

/**
 * Created by andrew_asa on 2018/7/28.
 * 结果集
 * 对应数据库中的表
 */
public interface DataSet extends Serializable {

    List<RowSet> getDataList();

    int size();

    Object getObject(int row, int column);
}
