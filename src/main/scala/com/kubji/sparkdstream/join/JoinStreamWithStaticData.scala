package com.kubji.sparkdstream.join

import com.kubji.sparkdstream.{Customer, Transaction}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, DoubleType, StringType, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._

object JoinStreamWithStaticData {

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

    val customerRddWithHeader = sparkSession.sparkContext
      .textFile(customerPath)

    val customerRdd = customerRddWithHeader.mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }.map(Customer.parse)

    val customerPairRdd = customerRdd.map(customer => (customer.cc_num, customer))

    val numPartions =  customerPairRdd.getNumPartitions
    val partitionedRdd = customerPairRdd.partitionBy(new HashPartitioner(numPartions)).cache()

    partitionedRdd.count()

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(10))

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
                       ).map(cr => cr.value)



    val joinedDStream = kafkaDStreams.transform(rdd => {

      val transactionPair = rdd.map(Transaction.parse).map(transaction => (transaction.cc_num, transaction))

      val joinedRdd = transactionPair.join(partitionedRdd)

      joinedRdd.map{
        case(cc_num, (t, c)) => (cc_num, t.transTime, t.category, t.merchant, t.amt, c.first, c.last, c.gender, c.city, c.state, c.job)
      }
    })

    joinedDStream.print()


    ssc.start()
    ssc.awaitTermination()

  }

}