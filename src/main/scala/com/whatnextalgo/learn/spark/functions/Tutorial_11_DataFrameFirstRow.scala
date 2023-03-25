package com.whatnextalgo.learn.spark.functions
import com.whatnextalgo.learn.spark.utils.Context
object Tutorial_11_DataFrameFirstRow extends App with Context{

  sparkSession.sparkContext.setLogLevel("ERROR")
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  val firstRow = df.first()
  println(s"First row = $firstRow")

  val firstRowColumn1 = df.first().get(0)
  println(s"First row column 1 = $firstRowColumn1")

  val firstRowColumnPrice = df.first().getAs[Double]("Price")
  println(s"First row column Price = $firstRowColumnPrice")

}
