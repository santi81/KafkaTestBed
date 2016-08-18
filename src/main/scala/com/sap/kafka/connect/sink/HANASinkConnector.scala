package com.sap.kafka.connect.sink

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.JavaConverters._

class HANASinkConnector extends SinkConnector{

  private var configProps : Option[util.Map[String, String]] = None
  private val connConfigDef : Option[ConfigDef] = None


  /**
   * States which SinkTask class to use
   * */
  override def taskClass(): Class[_ <: Task] = classOf[HANASinkTask]

  /**
   * Set the configuration for each work and determine the split
   *
   * @param maxTasks The max number of task workers be can spawn
   * @return a List of configuration properties per worker
   * */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {

    (1 to maxTasks).map(c => configProps.get).toList.asJava
  }

  /**
   * Start the sink and set to configuration
   *
   * @param props A map of properties for the connector and worker
   * */
  override def start(props: util.Map[String, String]): Unit = {
    println("Printing the Configuration properties")
    configProps.foreach(println)
    configProps = Some(props)
    ConnectUtils(props.asScala.toMap)
  }

  override def stop(): Unit = {}
  override def version(): String = getClass.getPackage.getImplementationVersion

  override def config(): ConfigDef = new ConfigDef


}
