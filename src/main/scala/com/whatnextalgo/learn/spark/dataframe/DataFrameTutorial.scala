package com.whatnextalgo.learn.spark.dataframe
import com.whatnextalgo.learn.spark.utils.Context


object DataFrameTutorial extends App with Context{

  val dfTags = sparkSession
    .read
    .option("header","true")
    .option("inferSchema","true")
    .format("csv")
    .load("src/main/scala/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)
}
