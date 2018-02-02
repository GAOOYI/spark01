package com.bigdata.sparksql.jsonDataSources;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class JSONDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JSONDataSource").set("spark.testing.memory", "481859200").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //针对JSON文件创建dataframe
        Dataset<Row> studentScoresDF = sqlContext.read().json("hdfs://centos01:9000/spark-study/students.json");
        //读取本地数据源
        //Dataset<Row> studentScoresDF = sqlContext.read().json("H:\\java\\Spark\\北风网Spark 2.0从入门到精通-278讲\\000.课程代码+软件包\\第80讲-Spark SQL：JSON数据源复杂综合案例实战\\文档\\students.json");


        //将创建的Dataset注册临时表，查询分数大于80的学生的姓名
        studentScoresDF.registerTempTable("student_scores");
        Dataset<Row> GT80student = sqlContext.sql("select name,score from student_scores where score >= 80");

        List<String> GT80studentName = GT80student.javaRDD().map(row -> row.getString(0)).collect();

        //针对JavaRDD<String>创建DataFrame
        ArrayList<String> studentInfoJSONS = new ArrayList<>();
        studentInfoJSONS.add("{\"name\":\"Leo\",\"age\":\"18\"}");
        studentInfoJSONS.add("{\"name\":\"Marry\",\"age\":\"19\"}");
        studentInfoJSONS.add("{\"name\":\"Jack\",\"age\":\"17\"}");

        JavaRDD<String> studentInfoRDD = sc.parallelize(studentInfoJSONS);
        //针对这个RDD，将其转换为Dataset
        Dataset<Row> studentInfoDs = sqlContext.read().json(studentInfoRDD);

        //针对这个Dataset注册临时表，查询
        studentInfoDs.registerTempTable("student_info");
        String sql = "select name,age from student_info where name in (";
        for (int i = 0; i < GT80studentName.size();i++){
            sql = sql + "'" +  GT80studentName.get(i) + "'";
            if (i < GT80studentName.size() -1){
                sql = sql + ",";
            }
        }
        sql += ")";

        //在studentInfoDs中查询
        Dataset<Row> GT80studentInfoDs = sqlContext.sql(sql);

        //将这个dataset转换为一个RDD，然后执行join操作，最后将得到的结果转换为一个Row的RDD
        JavaRDD<Row> JoinRow = GT80studentInfoDs.javaRDD()
                .mapToPair(row -> new Tuple2<>(row.getString(0), Integer.valueOf(row.getString(1))))
                .join(GT80student.javaRDD()
                        .mapToPair(row -> new Tuple2<>(row.getString(0), Integer.parseInt(String.valueOf(row.getLong(1))))))
                .map(t -> RowFactory.create(t._1, t._2._1, t._2._2));

        //将得到的RowRDD转换为一个Dataset
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> df = sqlContext.createDataFrame(JoinRow, structType);

        //将这个df文件保存到一个json文件到hdfs文件中
        df.write().format("json").mode(SaveMode.Append).save("hdfs://centos01:9000/spark-study/studentsInfo.json");
        df.show();

        sc.close();

    }
}
