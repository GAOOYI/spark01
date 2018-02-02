package com.bigdata.spark.countnumber;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class groupByKeyAndTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("groupByKeyAndTop3").setMaster("local").set("spark.testing.memory","481859200");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("H:\\sparkJar\\score.txt");

        lines.mapToPair(line -> new Tuple2<>(line.split(" ")[0],Integer.valueOf(line.split(" ")[1])))
                .groupByKey()
                .foreach(t -> {
                    System.out.print(t._1 + ":");
                    List<Integer> list = new ArrayList<>();
                    t._2.forEach(i-> list.add(i));
                    Collections.sort(list);
                    list.forEach(integer -> System.out.print(" " + integer));
                    System.out.println();
                });
    }
}
