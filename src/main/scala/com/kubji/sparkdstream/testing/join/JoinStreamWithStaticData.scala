/*
package com.kubji.sparkdstream.testing.join

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}


object JoinStreamWithStaticData {

  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val topic = args(1)
    val customerPath = args(2)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("NetworkWordCount")
      .set("spark.streaming.blockInterval", "5000ms")

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)


    val customerRdd = sparkContext.textFile(customerPath)
      .mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }
      .map(Customer.parse)

    val customerPariRdd = customerRdd.map(customerRecord => (customerRecord.cc_num, customerRecord))

    val numPartitions = customerPariRdd.getNumPartitions

    val customerPartitionedRdd = customerPariRdd.partitionBy(new HashPartitioner(numPartitions)).cache()
    customerPartitionedRdd.count()


    val ssc = new StreamingContext(sparkContext, Seconds(10))
    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "SingleReceiverGroup",
      "zookeeper.connection.timeout.ms" -> "1000")

    val topicMap = Map[String, Int](topic -> 1)

    val kafkaStreams = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY_SER).map(_._2)


    val joinedDStream = kafkaStreams.transform(rawRdd => {

      val rdd = sqlContext.read.json(rawRdd).rdd
      val transactionRdd = rdd.map(Transaction.parse)
      val transactionPair = transactionRdd.map(transaction => (transaction.cc_num, transaction))

      val joinRdd = transactionPair.join(customerPartitionedRdd)
        .map {
          case ((cc_num, (transaction, customer))) => {
            (cc_num, transaction.transId, transaction.transTime, transaction.amt, transaction.merchant, transaction.category, customer.first, customer.last, customer.gender, customer.job, customer.city)
          }
        }

      joinRdd
    })

    joinedDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}*/
