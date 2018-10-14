package com.asa.lab.internalimp.datasource;

import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.Type;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class BaseColumn implements Column {

    private Type type;

    private String name;

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
}