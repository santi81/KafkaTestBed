package io.gatling.kafka

import java.io.File

import akka.actor.{ActorSystem, Props}
import io.gatling.actors.DBActor
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.TimeHelper
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.stats.message.ResponseTimings
import org.apache.avro.Schema
import org.apache.commons.io.FileUtils

class KafkaProducerAction[K, V](producerProtocol: KafkaProducerProtocol[K, V],
                          statsEngine: StatsEngine,
                          nextAction: Action,
                                tableName: String,
                                keySchema: Option[Schema] = None,
                                valueSchema: Option[Schema] = None)
  extends ChainableAction {
  val directory = new File("target" + File.separator + "db")
  if (directory.exists()) {
    FileUtils.deleteDirectory(directory)
  }
  FileUtils.forceMkdir(directory)

  override def execute(session: Session): Unit = {
    val start = TimeHelper.nowMillis
    try {
      executeDBActor()
      producerProtocol.call(session, keySchema, valueSchema)
      val end = TimeHelper.nowMillis

      statsEngine.logResponse(session, "test",
        ResponseTimings(start, end), OK, Some("Success"), None)
    } catch {
      case e: Exception => statsEngine.logResponse(session, "test",
        ResponseTimings(start, start), KO, Some(e.getMessage), None)
    }
    nextAction ! session
  }

  override def next: Action = {
    nextAction
  }

  override def name: String = {
    getClass.getName
  }

  private def executeDBActor(): Unit = {
    val system = ActorSystem("DB-Actor-System")
    val dbActor = system.actorOf(Props[DBActor], name = "DBActor")
    dbActor ! tableName
  }
}
