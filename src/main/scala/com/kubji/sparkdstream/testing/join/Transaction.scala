package  com.kubji.sparkdstream.testing.join

import java.util.Date

import org.apache.spark.sql.Row


case class Transaction(cc_num:String, transId:String, transTime:Date, category:String, merchant:String, amt:Double, merchLatitude:Double, merchLongitude:Double)

object Transaction {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

  def parse(transactionRecord: String) = {

    val fields = transactionRecord.split(",")
    val cc_num = fields(0)
    val transId = fields(1)
    val transTime = format.parse(fields(2))
    val category = fields(3)
    val merchant = fields(4)
    val amt = fields(5).toDouble
    val lat = fields(6).toDouble
    val long = fields(7).toDouble
    val fraud = fields(8).toInt

    new Transaction(cc_num, transId, transTime, category, merchant, amt, lat, long)
  }


  def parse(row: Row) = {

    val cc_num = row.getAs[String]("cc_num")
    val transId = row.getAs[String]("trans_num")
    val transTime = format.parse(row.getAs[String]("trans_time"))
    val category = row.getAs[String]("category")
    val merchant = row.getAs[String]("merchant")
    val amt = row.getAs[String]("amt").toDouble
    val lat = row.getAs[String]("merch_lat").toDouble
    val long = row.getAs[String]("merch_long").toDouble

    new Transaction(cc_num, transId, transTime, category, merchant, amt, lat, long)
  }
}