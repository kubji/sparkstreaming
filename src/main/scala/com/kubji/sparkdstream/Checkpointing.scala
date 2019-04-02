package  com.kubji.sparkdstream

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Checkpointing {


  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val ipAddress = args(1)
    val port = args(2).toInt
    val checkpointingDirectory = args(3)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("RecoverableNetworkWordCount")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")


    def creatingFun():StreamingContext =  {
      println("New context created")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointingDirectory)

      val lines = ssc.socketTextStream(ipAddress, port, StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCount = pairs.reduceByKey(_ + _)

      wordCount.print()
      ssc

    }

    val ssc = StreamingContext.getOrCreate(checkpointingDirectory, creatingFun)

    ssc.start()
    ssc.awaitTermination()
  }
}