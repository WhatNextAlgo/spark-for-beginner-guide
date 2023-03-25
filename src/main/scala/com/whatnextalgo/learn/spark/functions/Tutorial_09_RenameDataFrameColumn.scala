package com.whatnextalgo.learn.spark.functions
import com.whatnextalgo.learn.spark.utils.Context
object Tutorial_09_RenameDataFrameColumn extends App with Context{
  sparkSession.sparkContext.setLogLevel("ERROR")
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price")
  df.show()

  val df2 = df.withColumnRenamed("Donut Name","Name")
  df2.show()

}
