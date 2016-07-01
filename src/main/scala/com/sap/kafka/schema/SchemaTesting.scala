package com.sap.kafka.schema

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, CachedSchemaRegistryClient}
import org.apache.avro.Schema

object SchemaTesting extends App{

  val   KEY_SERIALIZATION_FLAG = "key"
  val   VALUE_SERIALIZATION_FLAG = "value"
  val   schemaRegistryUrl = "http://10.97.136.161:8081"
  val topic = "kafka-avro-registry1"
  val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1)



  def getSchema(topic:String,Serilization: Boolean): SchemaMetadata = {

    var subject = ""
    subject = topic + "-" + VALUE_SERIALIZATION_FLAG
    val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
    println(metadata.getSchema)
    metadata
  }

  def doSchemaRegistry(topic: String, SerializationOption: Boolean,schema: Schema) : Boolean =   {
    var subject = ""
      subject = topic + "-" + VALUE_SERIALIZATION_FLAG
    schemaRegistryClient.register(subject, schema)
    true
  }




}
