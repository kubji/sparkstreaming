package com.kubji.sparkdstream.offsetmanagement

import com.datastax.driver.core.PreparedStatement
import com.datastax.spark.connector.cql.CassandraConnector
import com.kubji.sparkdstream.Transaction
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ExactlyOnceSemantics {

  def transactionPrepare() = {

    s"""
     insert into creditcard.transaction (
       cc_num,
       transId,
       transTime,
       category,
       merchant,
       amt,
       merchLatitude,
       merchLongitude
     )
     values(
       ?, ?, ?, ?, ?, ?, ?, ?
        )"""
  }


  def transactionBind(prepared: PreparedStatement, transaction: Transaction) = {
    val bind = prepared.bind()
    bind.setString("cc_num", transaction.cc_num)
    bind.setString("transId", transaction.transId)
    bind.setTimestamp("transTime", transaction.transTime)
    bind.setString("category", transaction.category)
    bind.setString("merchant", transaction.merchant)
    bind.setDouble("amt", transaction.amt)
    bind.setDouble("merchLatitude", transaction.merchLatitude)
    bind.setDouble("merchLongitude", transaction.merchLongitude)
  }



  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Exactly Once Semantics")
      .getOrCreate()

    import sparkSession.implicits._


    val connector = CassandraConnector(sparkSession.sparkContext.getConf)
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "ExactlyOnceSemanticsGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val topicSet = Set(topic)

    import com.datastax.spark.connector.streaming._

    val kafkaDStream = KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Subscribe[String, String](topicSet, kafkaParams)
                       )




    kafkaDStream.foreachRDD(rdd => {

      /* Extract Offset */
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offRange => {
        //logger.info("fromOffset: " + offRange.fromOffset + "untill Offset: " + offRange.untilOffset)
        println("Partition: " + offRange.partition + ", fromOffset: " + offRange.fromOffset + ", untill Offset: " + offRange.untilOffset)
      })

      rdd.foreachPartition(pItr => {
        connector.withSessionDo(session => {
          val prepared = session.prepare(transactionPrepare())

          pItr.foreach(cr => {
            val transaction = Transaction.parse(cr.value())
            session.execute(transactionBind(prepared, transaction))
          })

        })

      })

      /* After all processing is done, offset is committed to Kafka */
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()
  }
}