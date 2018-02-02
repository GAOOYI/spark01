package com.bigdata.spark.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ActionOpeator {
    public static void main(String[] args) {

    }

    @Test
    public void reduce(){
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local").set("spark.testing.memory","481859200");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7);

        JavaRDD<Integer> nums = sc.parallelize(list);
        System.out.println(nums.reduce(( i, j) -> i + j));

        sc.close();
    }
}
