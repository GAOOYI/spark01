package com.bigdata.sparksql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HiveDataSource").set("spark.testing.memory", "481859200");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());


        hiveContext.sql("DROP TABLE IF EXISTS student_infos");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING,  age INT)");
        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/hadoop/spark-study/resources/student_infos.txt' INTO TABLE student_infos");

        Dataset<Row> user = hiveContext.table("user");

        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING,  score INT)");
        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/hadoop/spark-study/resources/student_scores.txt' INTO TABLE student_scores");

        Dataset<Row> GT80 = hiveContext.sql("select s1.name, s1.age, s2.score " +
                "from student_infos s1 " +
                "join student_scores s2 " +
                "on s1.name=s2.name " +
                "where s2.score>=80");



        hiveContext.sql("DROP TABLE IF EXISTS goodstudent");
        GT80.registerTempTable("good_student");
        hiveContext.sql("create table goodstudent as select * from good_student");

        hiveContext.table("goodstudent").javaRDD().foreach(row -> System.out.println(row.toString()));
        sc.close();
    }
}
