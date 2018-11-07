package com.asa.lab.structure.datasource.relation;

import java.io.Serializable;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 * 给大表的行号获取小表的行号
 */
public interface RelationIndexMapping extends Serializable {

    /**
     * 根据大表的行获取小表的行
     * 不存在则返回-1
     *
     * @param primaryRowIndex
     * @return
     */
    int getForeignRowIndex(int primaryRowIndex);
}
