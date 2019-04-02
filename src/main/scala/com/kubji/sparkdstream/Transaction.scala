package  com.kubji.sparkdstream

import java.util.Date


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

    new Transaction(cc_num, transId, transTime, category, merchant, amt, lat, long)
  }
}