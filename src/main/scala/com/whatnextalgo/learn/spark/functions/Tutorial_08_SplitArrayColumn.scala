package com.whatnextalgo.learn.spark.functions
import com.whatnextalgo.learn.spark.utils.Context
object Tutorial_08_SplitArrayColumn extends App with Context{
  import sparkSession.sqlContext.implicits._
  sparkSession.sparkContext.setLogLevel("ERROR")
  val targets: Seq[(String, Array[Double])] = Seq(("Plain Donut", Array(1.50, 2.0)), ("Vanilla Donut", Array(2.0, 2.50)), ("Strawberry Donut", Array(2.50, 3.50)))
  val df = sparkSession
    .createDataFrame(targets)
    .toDF("Name", "Prices")

  df.show()

  df.printSchema()

  df
    .select(
      $"Name",
      $"Prices"(0) as "Low Price",
      $"Prices"(1) as "High Price"
    ).show()
}
