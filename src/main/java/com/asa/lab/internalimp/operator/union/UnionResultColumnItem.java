package com.asa.lab.internalimp.operator.union;

import com.asa.lab.structure.datasource.Type;
import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/18.
 * 合并结果列项
 */
public class UnionResultColumnItem {

    /**
     * 合并结果名字
     */
    private String name;

    /**
     * 合并结果类型
     */
    private Type type;

    public UnionResultColumnItem(String name, Type type) {

        this.name = name;
        this.type = type;
    }

    public String getName() {

        return name;
    }

    public void setName(String name) {

        this.name = name;
    }

    public Type getType() {

        return type;
    }

    public void setType(Type type) {

        this.type = type;
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
