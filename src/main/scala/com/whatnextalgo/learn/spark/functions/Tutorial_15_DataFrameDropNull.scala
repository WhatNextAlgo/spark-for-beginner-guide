package com.whatnextalgo.learn.spark.functions

import com.whatnextalgo.learn.spark.utils.Context

object Tutorial_15_DataFrameDropNull extends App with Context{
  sparkSession.sparkContext.setLogLevel("ERROR")
  val donuts = Seq(("plain donut", 1.50), (null.asInstanceOf[String], 2.0), ("glazed donut", 2.50))
  val dfWithNull = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  dfWithNull.show()

  val dfWithoutNull = dfWithNull.na.drop()

  dfWithoutNull.show()

}
