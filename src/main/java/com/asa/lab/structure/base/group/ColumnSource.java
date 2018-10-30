package com.asa.lab.structure.base.group;

/**
 * @author andrew_asa
 * @date 2018/10/29.
 * 列源头
 */
public interface ColumnSource {

    /**
     * 字段来源
     *
     * @return
     */
    String getColumnSourceName();

    /**
     * 显示名
     *
     * @return
     */
    String getDisplayName();
}
