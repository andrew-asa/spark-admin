package com.asa.lab.internalimp.datasource.relation.column;

import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.relation.RelationIndexMapping;
import com.asa.utils.ComparatorUtils;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 * 没有经过压缩的
 */
public class DefaultRelationIndexMapping implements RelationIndexMapping {

    private DataSet primaryDataSet;

    private int primaryColIndex;

    private DataSet foreignDataSet;

    private int foreignColIndex;

    //private LruCache<Object, Integer> indexCache;

    private int nullIndex = -1;

    private static final int CACHESIZE = 100;

    private static final int NOT_FIX_ROW = -1;

    private int foreignRowSize;

    public DefaultRelationIndexMapping(DataSet primaryDataSet, int primaryColIndex, DataSet foreignDataSet, int foreignColIndex) {

        this.primaryDataSet = primaryDataSet;
        this.primaryColIndex = primaryColIndex;
        this.foreignDataSet = foreignDataSet;
        this.foreignColIndex = foreignColIndex;
        init();
    }

    private void init() {

        //indexCache = new LruCache<>(CACHESIZE);
        foreignRowSize = foreignDataSet.getRowSize();
    }

    @Override
    public int getForeignRowIndex(int primaryRowIndex) {

        Object pvalue = primaryDataSet.getObject(primaryRowIndex, primaryColIndex);
        if (pvalue != null) {
            //Integer index = indexCache.get(pvalue);
            //if (index != null) {
            //    return index;
            //}
        } else {
            if (nullIndex != NOT_FIX_ROW) {
                return nullIndex;
            }
        }
        for (int i = 0; i < foreignRowSize; i++) {
            Object fvalue = foreignDataSet.getObject(i, foreignColIndex);
            if (ComparatorUtils.equals(fvalue, pvalue)) {
                if (fvalue != null) {
                    //indexCache.put(fvalue, i);
                } else {
                    nullIndex = i;
                }
                return i;
            }
        }
        return NOT_FIX_ROW;
    }
}
