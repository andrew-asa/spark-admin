package com.asa.lab.demo.filter;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;


/**
 * Created by andrew_asa on 2018/7/28.
 */
public class RDDTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setAppName("WordCounter")//
                .setMaster("local");

        String fileName = "/log4j.properties";

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(fileName, 1);

        JavaRDD<String> words = lines
                .flatMap(new FlatMapFunction<String, String>() {
                    private static final long serialVersionUID = 1L;

                    // 以前的版本好像是Iterable而不是Iterator
                    @Override
                    public Iterator<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                });

        JavaPairRDD<String, Integer> pairs = words
                .mapToPair(new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });

        JavaPairRDD<String, Integer> result = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer e, Integer acc)
                            throws Exception {
                        return e + acc;
                    }
                }, 1);

        result.map(
                new Function<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(
                            Tuple2<String, Integer> v1) throws Exception {
                        return new Tuple2<>(v1._1, v1._2);
                    }
                })//
                .sortBy(new Function<Tuple2<String, Integer>, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Tuple2<String, Integer> v1)
                            throws Exception {
                        return v1._2;
                    }
                }, false, 1)//
                .foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Tuple2<String, Integer> e)
                            throws Exception {
                        System.out.println("【" + e._1 + "】出现了" + e._2 + "次");
                    }
                });
        sc.close();
    }
}
