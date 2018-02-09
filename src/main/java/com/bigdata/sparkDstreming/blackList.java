package com.bigdata.sparkDstreming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;

public class blackList {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("blackList").setMaster("local[3]").set("spark.testing.memory", "481859200");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        ArrayList<Tuple2<String, Boolean>> bList = new ArrayList<>();
        bList.add(new Tuple2<>("leo",true));
        JavaPairRDD<String, Boolean> bRDD = jsc.sparkContext().parallelizePairs(bList);

        JavaReceiverInputDStream<String> logs = jsc.socketTextStream("localhost", 9999);
        logs.mapToPair(line -> new Tuple2<String,String>(line.split(" ")[1],line))
                .transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaPairRDD<String, String> logRDD) throws Exception {
                        return logRDD.leftOuterJoin(bRDD).filter(t->{
                            if (t._2._2().isPresent() && t._2._2.get())
                                return false;
                            else return true;
                        }).map(t -> t._2._1);
                    }
                })
                .print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
