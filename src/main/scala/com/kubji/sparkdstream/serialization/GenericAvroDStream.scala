package  com.kubji.sparkdstream.serialization

import com.kubji.avro.Transaction
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object GenericAvroDStream {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)


    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[org.apache.avro.generic.GenericRecord]))
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer", "1024k")
        .set("spark.kryoserializer.buffer.max", "1024m")


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Generic Avro Dstream")
      .config(conf)
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
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )

    val topicSet = Set(topic)


    val kafkaDStreams = KafkaUtils.createDirectStream[String, GenericRecord](ssc,
                       PreferConsistent,
                       Subscribe[String, GenericRecord](topicSet, kafkaParams)
                       ).map(cr => cr.value)

    val transactionDStream = kafkaDStreams.map(genericRecord => {
        val cc_num = genericRecord.get("cc_num")
        val transId = genericRecord.get("transId")
        val transTime = genericRecord.get("transTime")
        val category = genericRecord.get("category")
        val merchant = genericRecord.get("merchant")
        val amt = genericRecord.get("amt")
        val merchLatitude = genericRecord.get("merchLatitude")
        val merchLongitude = genericRecord.get("merchLongitude")
        (cc_num, transId, transTime, category, merchant, amt, merchLatitude, merchLongitude)
      })

    transactionDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}