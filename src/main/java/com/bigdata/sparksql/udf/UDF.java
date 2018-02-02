package com.bigdata.sparksql.udf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class UDF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UDF").set("spark.testing.memory", "481859200").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        ArrayList<String> name = new ArrayList<>();
        name.add("jack");
        name.add("tom");
        name.add("leo");

        JavaRDD<Row> nameRDD = sc.parallelize(name, 4).map(s -> RowFactory.create(s));

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(nameRDD, structType);

        dataFrame.registerTempTable("names");

        sqlContext.udf().register("le", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        },DataTypes.IntegerType);

        sqlContext.sql("select le(name) from names").javaRDD().foreach(row -> System.out.println(row.toString()));

    }
}
