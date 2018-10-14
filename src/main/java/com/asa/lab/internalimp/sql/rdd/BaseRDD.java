package com.asa.lab.internalimp.sql.rdd;

import com.asa.lab.structure.service.spark.SparkContentManager;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * @author andrew_asa
 * @date 2018/10/11.
 */
public class BaseRDD<T> extends RDD<T> {

    protected BaseRDD(Class cls) {

        super(SparkContentManager.getInstance().sparkContext(), new ArrayBuffer<>(), ClassTag$.MODULE$.apply(cls));
    }

    public BaseRDD(RDD<?> oneParent, ClassTag<T> evidence$2) {

        super(oneParent, evidence$2);
    }

    @Override
    public Iterator<T> compute(Partition split, TaskContext context) {

        return null;
    }

    @Override
    public Partition[] getPartitions() {

        return new Partition[0];
    }
}
