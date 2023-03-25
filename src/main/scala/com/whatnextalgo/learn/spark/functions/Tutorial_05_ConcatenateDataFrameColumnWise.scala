package com.whatnextalgo.learn.spark.functions
import com.whatnextalgo.learn.spark.utils.Context
object Tutorial_05_ConcatenateDataFrameColumnWise extends App with Context{
  sparkSession.sparkContext.setLogLevel("ERROR")
  val donuts = Seq(("111","plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333","glazed donut", 2.50))

  val dfDonuts = sparkSession
    .createDataFrame(donuts).toDF("Id","Donut Name","Price")

  dfDonuts.show()

  val inventory = Seq(("111",10),("222",20),("333",30))
  val dfInventory = sparkSession
    .createDataFrame(inventory).toDF("Id","Inventory")

  dfInventory.show()

  val dfDonutsInventory = dfDonuts.join(dfInventory,Seq("id"),"inner")
  dfDonutsInventory.show()
}
