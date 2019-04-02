package  com.kubji.sparkdstream.stateful

case class UserEvent(userId: Int, time:String, page: String, isLast: Boolean)


object  UserEvent {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")


  def parseEvent(event: String) = {

    val fields = event.split("\\s")
    val time = format.parse(fields(1))
    UserEvent(fields(0).toInt, fields(1), fields(2), fields(3).toBoolean)
  }
}