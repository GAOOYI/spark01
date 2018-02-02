package com.bigdata.sparksql.JDBCDataSource;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class jdbcData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("jdbcData").set("spark.testing.memory", "481859200").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String,String> options = new HashMap<>();
        options.put("url","jdbc:mysql://localhost:3366/ssm1");
        options.put("user","root");
        options.put("password","123");
        options.put("dbtable","user");

        Dataset<Row> user = sqlContext.read().format("jdbc").options(options).load();
        user.javaRDD().foreach(row -> System.out.println(row.toString()));

        user.javaRDD().map(row -> new Tuple2(row.getInt(0),row.getString(1)))
                .filter(tuple2 -> (Integer.valueOf(tuple2._1.toString()) > 20)?true:false)
                .foreach(tuple2 -> System.out.println(tuple2._1 + ":" + tuple2._2));

        //可以通过foreach方法逐条的连接jdbc连接数据库，将数据插入数据库
    }
}
