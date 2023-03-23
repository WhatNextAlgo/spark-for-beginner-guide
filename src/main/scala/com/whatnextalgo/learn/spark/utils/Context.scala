package com.whatnextalgo.learn.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
trait Context {

  val sparkConf = new SparkConf()
    .setAppName("Scala-for-beginner-guide")
    .setMaster("local[*]")
    .set("spark.cores.max","2")

  val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

}
