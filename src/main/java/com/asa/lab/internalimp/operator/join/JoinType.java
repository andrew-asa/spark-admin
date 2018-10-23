package com.asa.lab.internalimp.operator.join;

import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 汇总方式
 */
public enum JoinType {

    /**
     * 交集
     */
    inner("inner"),
    /**
     * 并集
     */
    outer("outer"),
    /**
     * 左合并
     */
    left("left"),
    /**
     * 右合并
     */
    right("right");

    private String name;

    JoinType(String name) {

        this.name = name;
    }

    public String getName() {

        return name;
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
