package com.asa.lab.internalimp.sql.rdd;

import org.apache.spark.Partition;

/**
 * @author andrew_asa
 * @date 2018/10/11.
 *
 */
public class BasePartition implements Partition {

    private int index;

    @Override
    public int index() {

        return index;
    }

    @Override
    public int hashCode() {

        return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {

        return super.equals(other);
    }

    public void setIndex(int index) {

        this.index = index;
    }
}
