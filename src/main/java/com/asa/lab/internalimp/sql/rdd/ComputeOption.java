package com.asa.lab.internalimp.sql.rdd;

import com.asa.utils.AssistUtils;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import scala.collection.Iterator;

/**
 * @author andrew_asa
 * @date 2018/10/12.
 * 计算相关参数
 */
public class ComputeOption {

    private Partition split;

    private TaskContext context;

    /**
     * 需要的列
     */
    private String[] requiredColumns;

    public ComputeOption(String[] requiredColumns) {
        this.requiredColumns = requiredColumns;
    }

    public ComputeOption(Partition split, TaskContext context, String[] requiredColumns) {

        this.split = split;
        this.context = context;
        this.requiredColumns = requiredColumns;
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

    public String[] getRequiredColumns() {

        return requiredColumns;
    }

    public void setRequiredColumns(String[] requiredColumns) {

        this.requiredColumns = requiredColumns;
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
