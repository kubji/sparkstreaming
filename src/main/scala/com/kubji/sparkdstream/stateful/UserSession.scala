package  com.kubji.sparkdstream.stateful

import java.io.FileWriter


case class UserSession(userEvents: Seq[UserEvent])


object  UserSession {

  def saveUserSession(state: Option[UserSession], outputPath:String) = {

    val fileWriter = new FileWriter(outputPath, true)
    state.foreach(session => fileWriter.write(session.toString + "\n"))
    fileWriter.close
  }


}