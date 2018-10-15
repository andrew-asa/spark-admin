package com.asa.lab.demo.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class DbDataFrameDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("spark session example")
                .getOrCreate();

        String url = "jdbc:mysql://localhost:3306/asa";
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "andrew")
                .option("user", "asa")
                .option("password", "asa")
                .load();
        df.show();
        df.printSchema();
    }
}
