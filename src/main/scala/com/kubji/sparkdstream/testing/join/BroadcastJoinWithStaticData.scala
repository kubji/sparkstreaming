/*
package com.kubji.sparkdstream.testing.join

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


object BroadcastJoinWithStaticData {


  def getCustomerHashMap(file: String) = {


    val customerMap = scala.collection.mutable.Map.empty[String, Array[String]]

    val lines = scala.io.Source.fromFile(file).getLines()

    while (lines.hasNext) {
      val line = lines.next()
      val customer = Customer.parse(line)
      val fields = line.split(",")
      val key = fields(0)
      val value = fields.drop(1)
      customerMap.put(key, value)
    }
    customerMap
  }


  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)
    val customerPath = args(2)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("NetworkWordCount")
      .set("spark.streaming.blockInterval", "5000ms")

    val sparkContext = new SparkContext(conf)
    val customerMap = getCustomerHashMap(customerPath)
    val customerBroadcast = sparkContext.broadcast(customerMap)


    val ssc = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "SingleReceiverGroup",
      "zookeeper.connection.timeout.ms" -> "1000")

    val topicMap = Map[String, Int](topic -> 1)

    val kafkaStreams = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY_SER).map(_._2)

    val joinedDStream = kafkaStreams.transform(rdd => {
      val transactionRdd = rdd.map(Transaction.parse)
      transactionRdd.map(transaction => {
        val cc_num = transaction.cc_num
        val customer = customerBroadcast.value.getOrElse(cc_num, Array.empty)
        (cc_num, transaction.transId, transaction.transTime, transaction.amt, transaction.merchant, transaction.category, customer(0), customer(1), customer(2), customer(9), customer(4))
      })
    })

    joinedDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}*/
