package com.asa.lab.demo.datasource;

import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.structure.resultset.Row;
import com.asa.lab.structure.resultset.Type;
import com.asa.lab.utils.datasource.DataSourceUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created by andrew_asa on 2018/8/3.
 * 内存测试
 */
public class MemoryDatasourceDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("MemoryDatasource")
                .master("local")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        MemoryDatasource datasource = DataSourceUtils.mockMemoryDatasource(
                new Type[]{Type.STRING, Type.STRING},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                });
        JavaRDD<Row> rowJavaRDD = jsc.parallelize(datasource.getSource().getDataList());
        rowJavaRDD.count();
        jsc.close();
    }
}
