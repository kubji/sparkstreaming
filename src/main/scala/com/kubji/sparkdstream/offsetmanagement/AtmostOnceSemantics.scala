package com.kubji.sparkdstream.offsetmanagement

import com.kubji.sparkdstream.Transaction
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object  AtmostOnceSemantics {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Join Stream With Static Data")
      .getOrCreate()

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "SparkDstreamkafkaManualOffsetCommit",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val topicSet = Set(topic)


    val kafkaDStream = KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Subscribe[String, String](topicSet, kafkaParams)
                       )


    kafkaDStream.foreachRDD(rdd => {

      /* Extract Offset */
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offRange => {
        //logger.info("fromOffset: " + offRange.fromOffset + "untill Offset: " + offRange.untilOffset)
        println("Partition: " + offRange.partition  + ", fromOffset: " + offRange.fromOffset + ", untill Offset: " + offRange.untilOffset)
      })

      /* Commit Offset before processing and saving data */
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)


      val transactionRdd = rdd.map(consumerRecord => {
        Transaction.parse(consumerRecord.value().toString)
      })

      transactionRdd.foreach(println)

    })

    ssc.start()
    ssc.awaitTermination()
  }
}