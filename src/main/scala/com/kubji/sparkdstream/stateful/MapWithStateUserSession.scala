package  com.kubji.sparkdstream.stateful

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._




object MapWithStateUserSession {

  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val ipAddress = args(1)
    val port = args(2).toInt
    val checkpointingDirectory = args(3)
    val outputPath = args(4)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("mapWithStateUserSession")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")


    def updateUserSesion(key: Int,
                         value: Option[UserEvent],
                         state: State[UserSession]): Option[UserSession] = {

      /*
      Get existing user events, or if this is the  first UserEvent then
      create an empty sequence of events.
      */
      val existingEvents: Seq[UserEvent] = state.getOption()
        .map(_.userEvents)
        .getOrElse(Seq[UserEvent]())


      if (value.isDefined) {
        /*Extract the new incoming value, appending the new event to the existing
        sequence of events.
        */
        val updatedUserSession: UserSession = value.map(newEvent => UserSession(newEvent +: existingEvents))
          .getOrElse(UserSession(existingEvents))


        /*
        Look for the end event. If found, save all the userEvents of the Session to external storage and return None
        If not, update the internal state and return the state
        */
        updatedUserSession.userEvents.find(_.isLast) match {
          case Some(_) =>
            state.remove()
            UserSession.saveUserSession(Some(updatedUserSession), outputPath)
            None
          case None =>
            state.update(updatedUserSession)
            Some(updatedUserSession)
        }
      } else {
        /*State expired by timeout. i.e value is not defined. Timeout is set to 1 minute for demo purpose. Usually timeout will be around 15-30 minutes
        If no userevent is rececived for a particular user for 1 minute then save all the userevents of that user to external storage and return None*/
        // Return None will delete the state automatically
        UserSession.saveUserSession(Some(UserSession(existingEvents)), outputPath)
        None
      }

    }


    val stateSpec =
      StateSpec
        .function(updateUserSesion _)
        .timeout(Minutes(1))



    def creatingFun():StreamingContext =  {
      println("New context created")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointingDirectory)

      val inputDStream = ssc.socketTextStream(ipAddress, port, StorageLevel.MEMORY_AND_DISK_SER)

      val userEventsDStream = inputDStream.map(UserEvent.parseEvent)
        .map(userEvent => (userEvent.userId, userEvent))

      val updatedSessionDStream = userEventsDStream.mapWithState(stateSpec)

      updatedSessionDStream.print()

      ssc

    }

    val ssc = StreamingContext.getOrCreate(checkpointingDirectory, creatingFun)

    ssc.start()
    ssc.awaitTermination()

  }
}