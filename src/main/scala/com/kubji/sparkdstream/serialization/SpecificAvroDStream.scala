package  com.kubji.sparkdstream.serialization

import com.kubji.avro.Transaction
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object  SpecificAvroDStream {

  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val topic = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Join Stream With Static Data")
      .getOrCreate()

    import sparkSession.implicits._


    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "SparkDstreamJoinStaticGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "io.confluent.kafka.serializers.KafkaAvroDeserializer",
      "schema.registry.url" -> "http://localhost:8081",
      "specific.avro.reader" -> "true",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )

    val topicSet = Set(topic)


    val kafkaDStreams = KafkaUtils.createDirectStream[String, Transaction](ssc,
                       PreferConsistent,
                       Subscribe[String, Transaction](topicSet, kafkaParams)
                       ).map(cr => cr.value)

    kafkaDStreams.foreachRDD(transactionRdd => {
      transactionRdd.foreach(println)

    })

    ssc.start()
    ssc.awaitTermination()
  }

}