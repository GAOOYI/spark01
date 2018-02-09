package com.bigdata.sparksql.top3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class Ptop3 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Ptop3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc.sc());

        //读取数据
        JavaRDD<String> messageRDD = sc.textFile("");


    }
}
