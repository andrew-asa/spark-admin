package com.asa.lab.internalimp.sql.rdd;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.Iterator;

/**
 * @author andrew_asa
 * @date 2018/10/12.
 */
public class ComputeOption {

    private Partition split;

    private TaskContext context;

    public ComputeOption(Partition split, TaskContext context) {

        this.split = split;
        this.context = context;
    }

    public Partition getSplit() {

        return split;
    }

    public void setSplit(Partition split) {

        this.split = split;
    }

    public TaskContext getContext() {

        return context;
    }

    public void setContext(TaskContext context) {

        this.context = context;
    }
}
