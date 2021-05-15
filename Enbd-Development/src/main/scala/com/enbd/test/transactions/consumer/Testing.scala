package com.enbd.test.Enbd_Development.consumer

import org.apache.spark.sql.SparkSession

object Testing extends App{
  System.setProperty("hadoop.home.dir", "C:\\winutils");
  val spark  = SparkSession.builder().appName("testing").master("local").getOrCreate()
  // val df1 = spark.read.option("header", "true").option("sep", ",").csv("c:/tmp/test.csv")
   
   val df = spark.createDataFrame(Seq((1, 2, 3), (4, 5, 6),
                                   (7, 8, 9))).toDF("col1", "col2", "col3")
     //df.show(false)
     
     
     
     
 
}


/*
 * 
 * +----+----+----+-----+
|col1|col2|col3|col4 |
+----+----+----+-----+
|1   |2   |3   |a b c|
|4   |5   |6   |d e f|
|7   |8   |9   |g h i|
+----+----+----+-----+
 * 
 * 
 * +----+----+----+----+|col1|col2|col3|col4|
+----+----+----+----+
|1   |2   |3   |a   |
|1   |2   |3   |b   |
|1   |2   |3   |c   |
|4   |5   |6   |d   |
|4   |5   |6   |e   |
|4   |5   |6   |f   |
|7   |8   |9   |g   |
|7   |8   |9   |h   |
|7   |8   |9   |i   |
+----+----+----+----+
 * 
 * 
 */
