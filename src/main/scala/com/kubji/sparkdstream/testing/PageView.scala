package com.kubji.sparkdstream.testing

import org.apache.spark.{SparkContext, SparkConf}


case class UserEvent(userId: Int, time:String, page: String, isLast: Boolean)


object PageView {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")


  def parseEvent(event: String) = {

    val fields = event.split(" ")
    val time = format.parse(fields(1))
    UserEvent(fields(0).toInt, fields(1), fields(2), fields(3).toBoolean)
  }

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("PageView")


    val sc = new SparkContext(conf)
    val dataRdd = sc.textFile(inputPath)


    val pageViewRdd = dataRdd.map(parseEvent)

    pageViewRdd.foreach(println)

  }
}