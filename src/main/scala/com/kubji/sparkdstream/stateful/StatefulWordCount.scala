package com.kubji.sparkdstream.stateful

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object StatefulWordCount {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val ipAddress = args(1)
    val port = args(2).toInt
    val checkpointingDirectory = args(3)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("RecoverableNetworkWordCount")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    
    def updateFunction(currentBatchCount: Seq[Int], runningCount: Option[Int]) = {
      val newCount = currentBatchCount.sum + runningCount.getOrElse(0)
      new Some(newCount)
    }

    def creatingFun():StreamingContext =  {
      println("New context created")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(30))
      ssc.checkpoint(checkpointingDirectory)

      val lines = ssc.socketTextStream(ipAddress, port, StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCount = pairs.reduceByKey(_ + _)
      val totalWordCount = wordCount.updateStateByKey(updateFunction)
      totalWordCount.print()
      ssc

    }

    val ssc = StreamingContext.getOrCreate(checkpointingDirectory, creatingFun)

    ssc.start()
    ssc.awaitTermination()
  }
}