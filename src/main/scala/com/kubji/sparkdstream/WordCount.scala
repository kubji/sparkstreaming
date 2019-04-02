package  com.kubji.sparkdstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCount {

  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val ipAddress = args(1)
    val port = args(2).toInt

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream(ipAddress, port)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCount = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCount.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate


  }

}