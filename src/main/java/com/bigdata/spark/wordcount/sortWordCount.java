package com.bigdata.spark.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class sortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sortWordCount").setMaster("local").set("spark.testing.memory","481859200");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("H:\\sparkJar\\spark.txt");
        lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .foreach(t -> System.out.println("word :" + t._1 + ",== " + t._2 + "times"));
        sc.close();
    }
}
