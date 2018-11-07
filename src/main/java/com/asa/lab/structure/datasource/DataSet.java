package com.asa.lab.structure.datasource;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

/**
 * Created by andrew_asa on 2018/7/28.
 * 结果集
 * 对应数据库中的表的数据
 */
public interface DataSet extends Serializable {

    Object getObject(int row, int column);

    int getColumnSize();

    int getRowSize();

    boolean getBoolean(int row, int column);

    int getInt(int row, int column);

    long getLong(int row, int column);

    float getFloat(int row, int column);

    double getDouble(int row, int column);

    String getString(int row, int column);

    Date getDate(int row, int column);

    Timestamp getTimestamp(int row, int column);
}
