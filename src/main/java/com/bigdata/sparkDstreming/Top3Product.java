package com.bigdata.sparkDstreming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;

public class Top3Product {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Top3Product")
                .set("spark.testing.memory", "481859200");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> clickLog = jsc.socketTextStream("localhost", 9999);

        //获取日志文件生成map
        clickLog.mapToPair(line -> new Tuple2<String, Integer>(line.split(" ")[1] + "_" + line.split(" ")[2],1))
                .reduceByKeyAndWindow((v1, v2) -> v1 + v2,Durations.seconds(60),Durations.seconds(10))
                .foreachRDD(rdd -> {
                    JavaRDD<Row> ca_pr_rowRDD = rdd.map(t -> RowFactory.create(t._1.split("_")[0], t._1.split(" ")[1], t._2));

                    ArrayList<StructField> structFields = new ArrayList<>();
                    structFields.add(DataTypes.createStructField("category", DataTypes.StringType,true));
                    structFields.add(DataTypes.createStructField("product", DataTypes.StringType,true));
                    structFields.add(DataTypes.createStructField("count_num", DataTypes.IntegerType,true));
                    StructType structType = DataTypes.createStructType(structFields);

                    SparkSession sparkSession = new SparkSession(jsc.sparkContext().sc());
                    //HiveContext hiveContext = new HiveContext(jsc.sparkContext());
                    Dataset<Row> df = sparkSession.createDataFrame(ca_pr_rowRDD, structType);
                    df.createTempView("ca_pr_table");
                    //sparkSession.registerDataFrameAsTable(df,"ca_pr_table");
                    sparkSession.sql("select category,product,count_num from (" +
                            "select category,product,count_num," +
                            "row_number() OVER (partition by category order by count_num DESC) rank from ca_pr_table" +
                            ") tmp where rank<=3").show();
                });
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
