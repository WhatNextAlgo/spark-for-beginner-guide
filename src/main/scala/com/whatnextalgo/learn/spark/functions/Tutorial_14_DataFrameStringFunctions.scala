package com.whatnextalgo.learn.spark.functions

import com.whatnextalgo.learn.spark.utils.Context

object Tutorial_14_DataFrameStringFunctions extends App with Context{

  sparkSession.sparkContext.setLogLevel("ERROR")
  val donuts = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price", "Purchase Date")

  import org.apache.spark.sql.functions._
  import sparkSession.sqlContext.implicits._

  df
    .withColumn("Contains plain", instr($"Donut Name", "donut"))
    .withColumn("Length", length($"Donut Name"))
    .withColumn("Trim", trim($"Donut Name"))
    .withColumn("LTrim", ltrim($"Donut Name"))
    .withColumn("RTrim", rtrim($"Donut Name"))
    .withColumn("Reverse", reverse($"Donut Name"))
    .withColumn("Substring", substring($"Donut Name", 0, 5))
    .withColumn("IsNull", isnull($"Donut Name"))
    .withColumn("Concat", concat_ws(" - ", $"Donut Name", $"Price"))
    .withColumn("InitCap", initcap($"Donut Name"))
    .show(false)
}
