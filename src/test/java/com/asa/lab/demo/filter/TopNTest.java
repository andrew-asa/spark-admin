package com.asa.lab.demo.filter;

import com.sun.rowset.internal.Row;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;


/**
 * Created by andrew_asa on 2018/7/28.
 */
public class TopNTest {

    @Test
    public void testTopN() {

        SparkConf conf = new SparkConf();

        conf.setAppName("WordCounter")//
                .setMaster("local");

        String fileName = "/Users/andrew_asa/Documents/lab/spark/src/main/resources/count.txt";

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(fileName, 1);
        JavaRDD<Row> rows = lines
                .map((Function<String, Row>) line -> {

                    String name = null;
                    String password = null;
                    if (StringUtils.isNotEmpty(line)) {
                        String[] words = line.split("\\s");
                        if (words.length > 1) {
                            name = words[0];
                            password = words[1];
                        }
                    }
                    Row ret = new Row(2, new String[]{

                            name, password
                    });
                    return ret;
                });
        JavaPairRDD<String, Iterable<Row>> groupRdd = rows.groupBy((Function<Row, String>) v1 -> (String) v1.getOrigRow()[0]);
        JavaPairRDD<String, Iterable<Row>> groupRddCache = groupRdd.cache();
        JavaPairRDD<String, Iterable<Row>> sortRdd = groupRddCache.sortByKey();
        sortRdd.collect();
         //groups = sortRdd.cache();

        //groups.forEach(tuple2 -> {
        //    String key = tuple2._1();
        //    Iterable<Row> row = tuple2._2();
        //    System.out.println("key=" + key);
        //});
        //JavaPairRDD<String, Iterable<Row>> sorts = grops.sortByKey();
        //sorts.collect();
        //Map<String, Long> counts =  sorts.countByKey();
        //Set<String> keys = counts.keySet();
        //keys.forEach(key->System.out.println("name:"+key+" password:"+counts.get(key)));
        sc.close();
    }
}
