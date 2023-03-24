package com.whatnextalgo.learn.spark.dataframe
import com.whatnextalgo.learn.spark.utils.Context
import org.apache.log4j.{Level, Logger}
object DataFrameTutorial extends App with Context{

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  sparkSession.sparkContext.setLogLevel("ERROR")

  val dfTags = sparkSession
    .read
    .option("header","true")
    .option("inferSchema","true")
    .format("csv")
    .load("src/main/scala/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)

  // Print DataFrame schema
  dfTags.printSchema()

  // Query dataframe: select column from a dataframe
  dfTags.select("id","tag").show(10)

  // DataFrame Query: filter by column value of a dataframe
  dfTags.filter("tag == 'php'").show(5)

  // DataFrame Query: count rows of a dataframe
  println(s"Number of php tags = ${dfTags.filter("tag == 'php'").count()}")

  // DataFrame Query: SQL like query
  dfTags.filter("tag like 's%'").show(10)

  // DataFrame Query: Multiple filter chaining
  dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)

  // DataFrame Query: SQL IN clause
  dfTags.filter("id in (25, 108)").show(10)

  // DataFrame Query: SQL Group By
  println("Group by tag value")
  dfTags.groupBy("tag").count().show(10)

  // DataFrame Query: SQL Group By with filter
  dfTags.groupBy("tag").count().filter("count > 5").show(10)

  // DataFrame Query: SQL order by
  dfTags.groupBy("tag").count().filter("count > 5").orderBy("tag").show(10)

  // DataFrame Query: Cast columns to specific data type
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("src/main/scala/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  dfQuestionsCSV.printSchema()

  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )



  dfQuestions.printSchema()
  dfQuestions.show(10)

  // DataFrame Query: Operate on a sliced dataframe
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()
  dfQuestionsSubset.show(5)

  // DataFrame Query: Join
  dfQuestionsSubset.join(dfTags,"id").show(5)

  // DataFrame Query: Join and select columns
  dfQuestionsSubset
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .show(10)

  // DataFrame Query: Join on explicit columns
  dfQuestionsSubset
    .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))
    .show(10)

  // DataFrame Query: Inner Join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "inner")
    .show(10)

  // DataFrame Query: Left Outer Join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "left_outer")
    .show(10)
  // DataFrame Query: Right Outer Join
  dfTags
    .join(dfQuestionsSubset, Seq("id"), "right_outer")
    .show(10)


  // DataFrame Query: Distinct
  dfTags
    .select("tag")
    .distinct()
    .show(10)


  sparkSession.stop()
}
