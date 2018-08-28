package com.asa.lab.structure.resultset;

import java.util.List;

/**
 * Created by andrew_asa on 2018/7/28.
 * 结果集
 * 对应数据库中的表
 */
public interface DataSet {

    Type[] getTypes();

    Row[] getDataArrary();

    List<Row> getDataList();

    int size();
}
