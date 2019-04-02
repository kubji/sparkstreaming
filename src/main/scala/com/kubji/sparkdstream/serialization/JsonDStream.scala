package com.kubji.sparkdstream.serialization

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, DoubleType, StringType, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object JsonDStream {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Join Stream With Static Data")
      .getOrCreate()

    import sparkSession.implicits._

    val transactionSchema = new StructType()
      .add("cc_num", StringType, true)
      .add("transId", StringType, true)
      .add("transTime", StringType, true)
      .add("category", StringType, true)
      .add("merchant", StringType, true)
      .add("amt", StringType, true)
      .add("merchLatitude", StringType, true)
      .add("merchLongitude", StringType, true)


    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "SparkDstreamJoinStaticGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )

    val topicSet = Set(topic)


    val kafkaDStreams = KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Subscribe[String, String](topicSet, kafkaParams)
                       ).map(cr => (cr.value()))


    val transactionDStream = kafkaDStreams.foreachRDD(rdd => {

      val transactionDF = rdd.toDF("value")
        .withColumn("transaction", from_json($"value", transactionSchema))
        .select("transaction.*")

      transactionDF.show(false)

    })

    ssc.start()
    ssc.awaitTermination()
  }
}