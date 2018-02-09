package com.bigdata.sparkDstreming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


public class HotWord {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("HotWord").setMaster("local[3]").set("spark.testing.memory", "481859200");
        JavaStreamingContext jss = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> searchLog = jss.socketTextStream("localhost", 9999);

        searchLog.mapToPair(line -> new Tuple2<String, Integer>(line.split(" ")[1],1))
                .reduceByKeyAndWindow((v1 ,v2)-> v1 + v2 ,Durations.seconds(60),Durations.seconds(10))
                .transform(rdd -> {
                    rdd.mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1))
                            .sortByKey(false)
                            .mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1))
                            .take(3)
                            .forEach(hot -> System.out.println("word:" + hot._1 + ",nums:" + hot._2));
                    return null;
                }).print();


        jss.start();
        jss.awaitTermination();
        jss.close();
    }
}
