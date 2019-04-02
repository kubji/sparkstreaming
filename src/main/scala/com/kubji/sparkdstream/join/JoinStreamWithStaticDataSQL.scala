package com.kubji.sparkdstream.join

import com.kubji.sparkdstream.Transaction
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, DoubleType, StringType, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object JoinStreamWithStaticDataSQL {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)
    val customerPath = args(2)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Join Stream With Static Data")
      .config("spark.streaming.blockInterval", "5000ms")
      .getOrCreate()

    import sparkSession.implicits._

    val customerSchema = new StructType()
      .add("cc_num", StringType, true)
      .add("first", StringType, true)
      .add("last", StringType, true)
      .add("gender", StringType, true)
      .add("street", StringType, true)
      .add("city", StringType, true)
      .add("state", StringType, true)
      .add("zip", StringType, true)
      .add("lat", DoubleType, true)
      .add("long", DoubleType, true)
      .add("job", StringType, true)
      .add("dob", TimestampType, true)


    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")
    val customerDF = sparkSession.read.format("csv").options(parseOptions).load(customerPath)



    val transactionSchema = new StructType()
      .add("cc_num", StringType, true)
      .add("trans_num", StringType, true)
      .add("trans_time", StringType, true)
      .add("category", StringType, true)
      .add("merchant", StringType, true)
      .add("amt", StringType, true)
      .add("merch_lat", StringType, true)
      .add("merch_long", StringType, true)


    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(10))

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "SparkDstreamJoinStaticSQLGroup",
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



    val transactionDStream = kafkaDStreams.transform(rdd => {

      val transactionDF = rdd.toDF("value")
        .withColumn("transaction", from_json($"value", transactionSchema))
        .select("transaction.*")

      val joinedDF = transactionDF.join(customerDF, "cc_num")

      joinedDF.rdd

    })

    ssc.start()
    ssc.awaitTermination()
  }
}