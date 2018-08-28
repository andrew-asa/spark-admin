package com.asa.lab.structure.resultset;

import java.util.Date;

/**
 * Created by andrew_asa on 2018/7/30.
 */
public interface Row {

    String getString(int i);

    double getDouble(int i);

    int getInt(int i);

    long getLong(int i);

    Date getDate(int i);

    int size();
}
