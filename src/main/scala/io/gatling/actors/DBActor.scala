package io.gatling.actors

import java.io.File
import java.sql.DriverManager

import akka.actor.Actor
import org.apache.commons.io.FileUtils

class DBActor extends Actor {
  val connection = DriverManager.getConnection("jdbc:sap://mo-91f787f9d.mo.sap.corp:30015/?autocommit=false",
    "SYSTEM", "Toor1234")
  val statement = connection.createStatement()

  override def receive = {
    case tableName =>
      val rs = statement.executeQuery(s"select count(*) from $tableName")
      rs.next()
      val count = rs.getInt(1)

      FileUtils.write(new File("target" + File.separator + "db" + File.separator + "records"),
        System.currentTimeMillis() + " - " + count + "\n", true)
  }
}