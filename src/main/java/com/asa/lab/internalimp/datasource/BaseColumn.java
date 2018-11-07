package com.asa.lab.internalimp.datasource;

import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.Type;

import java.io.Serializable;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class BaseColumn implements Column, Serializable {

    private Type type;

    private String name;

    private String tableName;

    public BaseColumn(Type type, String name) {

        this.type = type;
        this.name = name;
    }

    @Override
    public Type getType() {

        return type;
    }

    @Override
    public String getName() {

        return name;
    }

    public void setType(Type type) {

        this.type = type;
    }

    public void setName(String name) {

        this.name = name;
    }

    public String getTableName() {

        return tableName;
    }

    public void setTableName(String tableName) {

        this.tableName = tableName;
    }
}
