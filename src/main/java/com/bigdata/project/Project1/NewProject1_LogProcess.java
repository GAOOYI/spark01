package com.bigdata.project.Project1;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.util.ArrayList;

public class NewProject1_LogProcess {
    public static void main(String[] args) {
        String warehouseLocation = "F:\\spark\\spark_update";
        SparkSession session = new SparkSession.Builder()
                .appName("avgSaleary")
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .getOrCreate();



        JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

        ArrayList<String> studentInfoJSONS = new ArrayList<>();
        studentInfoJSONS.add("{\"name\":\"Leo\",\"age\":\"18\"}");
        studentInfoJSONS.add("{\"name\":\"Marry\",\"age\":\"19\"}");
        studentInfoJSONS.add("{\"name\":\"Jack\",\"age\":\"17\"}");
        JavaRDD<String> rdd = sc.parallelize(studentInfoJSONS);



        session.close();
    }
}
