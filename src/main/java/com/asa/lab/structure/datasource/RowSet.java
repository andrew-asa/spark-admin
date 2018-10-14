package com.asa.lab.structure.datasource;

import java.math.BigDecimal;
import java.sql.Date;

/**
 * Created by andrew_asa on 2018/7/30.
 */
public interface RowSet {

    String getString(int i);

    Date getDate(int i);

    boolean isNullAt(int ordinal);

    boolean getBoolean(int ordinal);

    byte getByte(int ordinal);

    short getShort(int ordinal);

    int getInt(int ordinal);

    long getLong(int ordinal);

    float getFloat(int ordinal);

    double getDouble(int ordinal);

    byte[] getBinary(int ordinal);

    BigDecimal getDecimal(int i);

    Object[] toArrary();

    int size();

    Object apply(int i);

    Object get(int i);

    Column[] getColumns();
}
