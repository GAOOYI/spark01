package com.bigdata.project.Project1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class project1_LogProcess {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("project1_LogProcess").setMaster("local")
                .set("spark.testing.memory", "481859200");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //读取数据
        JavaRDD<String> lines = jsc.textFile("C:\\Users\\Gaoyi\\Desktop\\access.log",6);

        //处理数据
        lines.mapToPair(line -> {                 //转换为key - value 对形式
            access_log log = new access_log(Long.parseLong(line.split("\t")[2])
                    , Long.parseLong(line.split("\t")[3])
                    , Long.parseLong(line.split("\t")[0]));
            return new Tuple2<String, access_log>(line.split("\t")[1], log);
        }).reduceByKey((access_log1, access_log2) -> {   //将upFlow和downFlow根据id加起来
            return new access_log(access_log1.getUpFlow() + access_log2.getUpFlow()
                    , access_log1.getDownFlow() + access_log2.getDownFlow()
                    , (access_log1.getTimeStamp() < access_log2.getTimeStamp()) ? access_log1.getTimeStamp() : access_log2.getTimeStamp());
        }).mapToPair(t -> {  //转换为可以排序的pair
            sortedByKey sorted = new sortedByKey(t._2.getUpFlow(), t._2.getDownFlow(), t._2.getTimeStamp());
            return new Tuple2<sortedByKey, String>(sorted, t._1);
        }).sortByKey(false)
                .take(10).forEach(t -> {
                    //打印前十
            System.out.println(t._1);
        });

        //关闭
        jsc.close();
    }
}
