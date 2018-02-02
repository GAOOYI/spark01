package com.bigdata.spark.transformation;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TransformationOperation {
    public static void main(String[] args) {
        //map();
        //filter();
        //sortByKey();
        cogroup();
    }

    /**
     * map操作
     */
    public static void map(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local").set("spark.testing.memory","481859200");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7);

        JavaRDD<Integer> numRDD = sc.parallelize(list);

        JavaRDD<Integer> mutipleNum = numRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        mutipleNum.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


    }

    /**
     * filter操作
     */
    public static void filter(){
        SparkConf conf = new SparkConf().setAppName("filter").setMaster("local").set("spark.testing.memory","481859200");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7);

        JavaRDD<Integer> numRDD = sc.parallelize(list);

        JavaRDD<Integer> evenRdd = numRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer n) throws Exception {
                return n % 2 == 0 ? true : false;
            }
        });

        evenRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /**
     * sortBykey
     * 使用java8新特性lambda表达式
     */
    public static void sortByKey(){
        SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local").set("spark.testing.memory","481859200");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> scoresList = Arrays.asList(new Tuple2<>(65, "leo"),
                new Tuple2<>(70, "tom"),
                new Tuple2<>(85, "jack"),
                new Tuple2<>(70, "java"));
        JavaPairRDD<Integer, String> scoreRDD = sc.parallelizePairs(scoresList);

        JavaPairRDD<Integer, String> sortedScores = scoreRDD.sortByKey(false);

        sortedScores.foreach(t -> System.out.println(t._1 + ": " + t._2));

        sc.close();

    }

    /**
     * cogroup
     */
    public static void cogroup(){
        SparkConf conf = new SparkConf().setAppName("cogroup").setMaster("local").set("spark.testing.memory","481859200");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> nameList = Arrays.asList(
                new Tuple2<>(4, "leo"),
                new Tuple2<>(1, "tom"),
                new Tuple2<>(2, "jack"),
                new Tuple2<>(3, "java"),
                new Tuple2<>(1,"spark"));
        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<>(1, 50),
                new Tuple2<>(2, 60),
                new Tuple2<>(3, 70),
                new Tuple2<>(4, 80),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 80));
        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoresList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = nameRDD.cogroup(scoreRDD);

        cogroup.foreach(c -> {
            System.out.print(c._1 + ":");
            c._2._1.forEach(c1 -> System.out.print(c1 + ","));
            System.out.print(" : ");
            c._2._2.forEach(c2 -> System.out.print(c2 + ","));
            System.out.println("==============");
        });

    }


}
