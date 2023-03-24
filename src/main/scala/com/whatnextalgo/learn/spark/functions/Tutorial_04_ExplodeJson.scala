package com.whatnextalgo.learn.spark.functions
import com.whatnextalgo.learn.spark.utils.Context
import org.apache.spark.sql.functions._
object Tutorial_04_ExplodeJson extends App with Context{
  import sparkSession.sqlContext.implicits._
  sparkSession.sparkContext.setLogLevel("ERROR")


  val tagDF = sparkSession
    .read
    .option("multiline","true")
    .option("inferSchema","true")
    .json("src/main/scala/resources/tags_sample.json")

  tagDF.show()

  val df = tagDF.select(explode($"stackoverflow") as "stackoverflow_tags")
  df.printSchema()

  df.select(
    $"stackoverflow_tags.tag.id" as "id",
          $"stackoverflow_tags.tag.author" as "author",
          $"stackoverflow_tags.tag.name" as "tag_name",
          $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
          $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  ).show()
}
