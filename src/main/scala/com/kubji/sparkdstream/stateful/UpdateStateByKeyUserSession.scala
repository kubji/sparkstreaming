package  com.kubji.sparkdstream.stateful

import java.io.FileWriter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object UpdateStateByKeyUserSession {



  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val ipAddress = args(1)
    val port = args(2).toInt
    val checkpointingDirectory = args(3)
    val outputPath = args(4)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("updateStateByKeyUserSession")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")


    def updateUserSesion(newEvents: Seq[UserEvent], state: Option[UserSession]): Option[UserSession] = {
      /*
      If this is the first set of events of a User then create a new UserSession with these events.
      Otherwise append the new events to the existing UserSession state.
      */

      val newState = state
        .map(prev => UserSession(prev.userEvents ++ newEvents))
        .orElse(Some(UserSession(newEvents)))

      /*
      If we received the `isLast` event in the current batch, save the session to the underlying store and return None to delete the state.
      Otherwise, return the accumulated state so that we can keep updating the state till isLast flag is received.
      */

      if (newEvents.exists(_.isLast)) {
        UserSession.saveUserSession(state, outputPath)
        None
      } else newState
    }


    def creatingFun():StreamingContext =  {
      println("New context created")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointingDirectory)

      val inputDStream = ssc.socketTextStream(ipAddress, port, StorageLevel.MEMORY_AND_DISK_SER)

      val userEventsDStream = inputDStream.map(UserEvent.parseEvent)
        .map(userEvent => (userEvent.userId, userEvent))


      val updatedUserSession = userEventsDStream.updateStateByKey(updateUserSesion)

      updatedUserSession.print()

      ssc

    }

    val ssc = StreamingContext.getOrCreate(checkpointingDirectory, creatingFun)

    ssc.start()
    ssc.awaitTermination()

  }
}