package io.gatling.actors

import java.io.File
import java.sql.DriverManager

import akka.actor.Actor
import org.apache.commons.io.FileUtils

class DBActor extends Actor {
  protected val driver: String = "sap.hanavora.jdbc.VoraDriver"
  private val jdbcUrl =
    s"""jdbc:hanavora://mo-695ecfd12.mo.sap.corp:30799""".stripMargin
  Class.forName(driver).newInstance()
  val connection = DriverManager.getConnection(jdbcUrl, System.getProperties)
  val statement = connection.createStatement()

  override def receive = {
    case tableName =>
      val rs = statement.executeQuery(s"select count(*) from $tableName")
      rs.next()
      val count = rs.getLong(1)

      FileUtils.write(new File("target" + File.separator + "db" + File.separator + "records"),
        System.currentTimeMillis() + " - " + count + "\n", true)
  }
}