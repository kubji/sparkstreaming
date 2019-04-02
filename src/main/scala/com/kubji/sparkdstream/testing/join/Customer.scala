package  com.kubji.sparkdstream.testing.join

import java.util.Date


case class Customer(cc_num:String, first: String, last:String, gender:String, street:String, city:String, state:String, zip:String, lat:String, long:String, job:String, dob:Date)

object Customer {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

  def parse(customerRecord:String) = {

    val fields = customerRecord.split(",")
    val cc_num = fields(0)
    val first = fields(1)
    val last = fields(2)
    val gender = fields(3)
    val street = fields(4)
    val city = fields(5)
    val state = fields(6)
    val zip = fields(7)
    val lat = fields(8)
    val long = fields(9)
    val job = fields(10)
    val dob = format.parse(fields(11))

    new Customer(cc_num, first, last, gender, street, city, state, zip, lat, long, job, dob)
  }
}