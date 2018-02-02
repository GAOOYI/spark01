package com.bigdata.spark.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class woedCountMain {
    public static void main(String[] args) {

        // 编写Spark应用程序
        // 本地执行，是可以执行在eclipse中的main方法中，执行的

        // 第一步：创建SparkConf对象，设置Spark应用的配置信息
        // 使用setMaster()可以设置Spark应用程序要连接的Spark集群的master节点的url
        // 但是如果设置为local则代表，在本地运行
        SparkConf sparkConf = new SparkConf().setAppName("woedCountMain").setMaster("local").set("spark.testing.memory","481859200");

        // 第二步：创建JavaSparkContext对象
        // 在Spark中，SparkContext是Spark所有功能的一个入口，你无论是用java、scala，甚至是python编写
        // 都必须要有一个SparkContext，它的主要作用，包括初始化Spark应用程序所需的一些核心组件，包括
        // 调度器（DAGSchedule、TaskScheduler），还会去到Spark Master节点上进行注册，等等
        // 一句话，SparkContext，是Spark应用中，可以说是最最重要的一个对象
        // 但是呢，在Spark中，编写不同类型的Spark应用程序，使用的SparkContext是不同的，如果使用scala，
        // 使用的就是原生的SparkContext对象
        // 但是如果使用Java，那么就是JavaSparkContext对象
        // 如果是开发Spark SQL程序，那么就是SQLContext、HiveContext
        // 如果是开发Spark Streaming程序，那么就是它独有的SparkContext
        // 以此类推
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        // 第三步：要针对输入源（hdfs文件、本地文件，等等），创建一个初始的RDD
        // 输入源中的数据会打散，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集
        // 我们这里呢，因为是本地测试，所以呢，就是针对本地文件
        // SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile()方法
        // 在Java中，创建的普通RDD，都叫做JavaRDD
        // 在这里呢，RDD中，有元素这种概念，如果是hdfs或者本地文件呢，创建的RDD，每一个元素就相当于
        // 是文件里的一行
        final JavaRDD<String> file = context.textFile("H:\\java\\Spark\\北风网Spark 2.0从入门到精通-278讲\\000.课程代码+软件包\\第29讲-Spark核心编程：使用Java、Scala和spark-shell开发wordcount程序\\文档\\spark.txt");

        // 第四步：对初始RDD进行transformation操作，也就是一些计算操作
        // 通常操作会通过创建function，并配合RDD的map、flatMap等算子来执行
        // function，通常，如果比较简单，则创建指定Function的匿名内部类
        // 但是如果function比较复杂，则会单独创建一个类，作为实现这个function接口的类

        // 先将每一行拆分成单个的单词
        // FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
        // 我们这里呢，输入肯定是String，因为是一行一行的文本，输出，其实也是String，因为是每一行的文本
        // 这里先简要介绍flatMap算子的作用，其实就是，将RDD的一个元素，给拆分成一个或多个元素
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("word:" + stringIntegerTuple2._1 + " ---- times:" + stringIntegerTuple2._2);
            }
        });

        context.close();
    }
}
