package com.whatnextalgo.learn.spark.streaming

import com.whatnextalgo.learn.spark.utils.Context
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StreamingDataFrames extends App with Context{
  sparkSession.sparkContext.setLogLevel("ERROR")

  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Option[Long],
                  Displacement: Option[Double],
                  Horsepower: Option[Long],
                  Weight_in_lbs: Option[Long],
                  Acceleration: Option[Double],
                  Year: String,
                  Origin: String
                )
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  def readFromSocket() = {
    import sparkSession.implicits._
    // reading a DF
    val lines: DataFrame = sparkSession.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 1234)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.*")



    // tell between a static vs a streaming DF
    println(lines.isStreaming)

    // consuming a DF
    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // wait for the stream to finish
    query.awaitTermination()
  }

  readFromSocket()

}
