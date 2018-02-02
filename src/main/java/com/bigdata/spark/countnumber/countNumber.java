package com.bigdata.spark.countnumber;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class countNumber {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("countNumber").setMaster("local").set("spark.testing.memory","481859200");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("H:\\sparkJar\\spark.txt");

        JavaRDD<Integer> count = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.trim().length();
            }
        });

        Integer number = count.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        System.out.println("The number of spark.txt words is " + number);

    }
}
