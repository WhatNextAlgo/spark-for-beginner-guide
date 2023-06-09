package com.whatnextalgo.learn.spark.dataframe
import org.apache.spark.sql.Dataset
import com.whatnextalgo.learn.spark.utils.Context
import org.apache.spark.sql.Row
object DataFrameOperations extends App with Context{
  sparkSession.sparkContext.setLogLevel("ERROR")

  // Setup
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/scala/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", false)
    .option("inferSchema", true)
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("src/main/scala/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV
    .filter("score > 400 and score < 410")
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .toDF()

  dfQuestions.show(10)

  // Convert DataFrame row to Scala case class
  case class Tag(id:Int, tag:String)
  import sparkSession.implicits._
  val dfTagsOfTag:Dataset[Tag] = dfTags.as[Tag]

  dfTagsOfTag
    .take(10)
    .foreach(t => println(s"id = ${t.id}, tag = ${t.tag}"))

  // DataFrame row to Scala case class using map()
  case class Question(owner_userid: Int, tag: String, creationDate: java.sql.Timestamp, score: Int)

  // create a function which will parse each element in the row
  def toQuestion(row:Row):Question = {
    // to normalize our owner_userid data
    val IntOf:String => Option[Int] = _ match {
      case s if s == "NA" => None
      case s => Some(s.toInt)
    }

    import java.time._
    val DateOf: String => java.sql.Timestamp = _ match {
      case s => java.sql.Timestamp.valueOf(ZonedDateTime.parse(s).toLocalDateTime)
    }
    Question(
      owner_userid = IntOf(row.getString(0)).getOrElse(-1),
      tag = row.getString(1),
      creationDate = DateOf(row.getString(2)),
      score = row.getString(3).toInt
    )

  }

  // now let's convert each row into a Question case class

  val dfOfQuestion:Dataset[Question]= dfQuestions.map(row => toQuestion(row))
  dfOfQuestion
    .take(10)
    .foreach(q => println(s"owner userid = ${q.owner_userid}, tag = ${q.tag}, creation date = ${q.creationDate}, score = ${q.score}"))

  // Create DataFrame from collection
  val seqTags = Seq(
    1 -> "so_java",
    1 -> "so_jsp",
    2 -> "so_erlang",
    3 -> "so_scala",
    3 -> "so_akka"
  )

  import sparkSession.implicits._
  val dfMoreTags = seqTags.toDF("id","tag")
  dfMoreTags.show(5)
  // DataFrame Union
  val dfUnionOfTags = dfTags
    .union(dfMoreTags)
    .filter("id in (1,3)")
  dfUnionOfTags.show(10)


  // DataFrame Intersection
  val dfIntersectionTags = dfMoreTags union dfUnionOfTags
  dfIntersectionTags show 5

  // Append column to DataFrame using withColumn()
  import org.apache.spark.sql.functions.split

  val dfSplitColumn = dfMoreTags
    .withColumn("tmp",split($"tag","_"))
    .select(
      $"id",
            $"tag",
            $"tmp".getItem(0).as("so_prefix"),
            $"tmp".getItem(1).as("so_tag")
    ).drop("tmp")

  dfSplitColumn.show(10)

  sparkSession.stop()
}
