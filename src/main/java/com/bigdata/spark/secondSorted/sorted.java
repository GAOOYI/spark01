package com.bigdata.spark.secondSorted;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class sorted {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sorted").setMaster("local").set("spark.testing.memory","481859200");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("H:\\sparkJar\\numbers.txt");

        lines.mapToPair(line -> new Tuple2<>(new sortedBykey(Integer.valueOf(line.split(" ")[0]),
                Integer.valueOf(line.split(" ")[1])),line)).sortByKey()
                .foreach(x-> System.out.println(x._2));
        sc.close();
    }
}
