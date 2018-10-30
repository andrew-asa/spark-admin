package com.asa.lab.structure.base.group;

import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 汇总方式
 */
public enum GroupType {

    /**
     * 文本|数值|时间相同值为一组
     */
    ID("id"),
    /**
     * 文本|时间定义分组
     */
    CUSTOM("custom"),
    /**
     * 数值
     */
    NUMBER_CUSTOM("number_custom"),

    /**
     * 数值自动分组
     */
    NUMBER_AUTO("number_auto");

    private String name;

    GroupType(String name) {

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
