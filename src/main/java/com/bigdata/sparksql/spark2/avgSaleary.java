package com.bigdata.sparksql.spark2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class avgSaleary {
    public static void main(String[] args) {
        String warehouseLocation = "F:\\spark\\spark_update";
        SparkSession sparkSession = new SparkSession.Builder()
                .appName("avgSaleary")
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .getOrCreate();

        Dataset<Row> department = sparkSession.read().json("F:\\spark\\department.json");
        Dataset<Row> employee = sparkSession.read().json("F:\\spark\\employee.json");

        employee.filter("age>20")
                .join(department, employee.col("depId").equalTo(department.col("id")))
                .groupBy(department.col("name"),employee.col("gender"))
                //.groupBy("name","gender")
                .avg("salary","age")
                .show();

        sparkSession.close();
    }
}
