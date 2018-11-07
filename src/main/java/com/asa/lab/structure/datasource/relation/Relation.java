package com.asa.lab.structure.datasource.relation;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/11/6.
 * 目前只支持单关联
 */
public interface Relation {

    /**
     * 大表
     *
     * @return
     */
    String getPrimaryTable();

    /**
     * 大表关联字段
     *
     * @return
     */
    List<String> getPrimaryFields();

    /**
     * 小表
     *
     * @return
     */
    String getForeignTable();

    /**
     * 小表关联字段
     *
     * @return
     */
    List<String> getForeignFields();
}
