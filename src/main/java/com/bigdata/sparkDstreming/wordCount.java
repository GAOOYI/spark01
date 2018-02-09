package com.bigdata.sparkDstreming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class wordCount {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("wordCount")
                .setMaster("local[2]").set("spark.testing.memory","481859200");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        //创建一个scoket通信线程
        JavaReceiverInputDStream<String> jrids = jsc.socketTextStream("localhost", 9999);
        jrids.print();
        Thread.sleep(10);
        //对获取的数据进行处理
        jrids.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<String,Integer>(word,1))
                .reduceByKey((t1,t2) -> (t1 + t2))
                .print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
