package com.whatnextalgo.learn.spark.functions
import com.whatnextalgo.learn.spark.utils.Context
import org.apache.spark.sql.functions._
object Tutorial_06_DataFrameColumnArrayContains extends App  with Context{
  import sparkSession.sqlContext.implicits._
  sparkSession.sparkContext.setLogLevel("ERROR")


  val tagsDF = sparkSession
    .read
    .option("multiline","true")
    .option("inferSchema","true")
    .json("src/main/scala/resources/tags_sample.json")

  val df = tagsDF
    .select(explode($"stackoverflow") as "stackoverflow_tags")
    .select(
      $"stackoverflow_tags.tag.id" as "id",
      $"stackoverflow_tags.tag.author" as "author",
      $"stackoverflow_tags.tag.name" as "tag_name",
      $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
      $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
    )
  df.show()

  df.select("*")
    .where(array_contains($"frameworks_name","Play Framework"))
    .show(false)


}
