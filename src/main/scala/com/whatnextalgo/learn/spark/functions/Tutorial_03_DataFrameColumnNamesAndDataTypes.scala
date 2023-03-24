package com.whatnextalgo.learn.spark.functions
import com.whatnextalgo.learn.spark.utils.Context
object Tutorial_03_DataFrameColumnNamesAndDataTypes extends App with Context{

  sparkSession.sparkContext.setLogLevel("ERROR")
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  val (columnNames, columnDataTypes) = df.dtypes.unzip
  println(s"DataFrame column names = ${columnNames.mkString(", ")}")
  println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")

}
