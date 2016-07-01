package com.sap.kafka.schema

import org.codehaus.jackson.map.ObjectMapper

object SchemaGenerator extends App{

  var schema = "{" +
    "\"schema\":" +
    "\"{" +
    "\"type\": \"record\"," +
    "\"name\": \"test\"," +
    "\"fields\":" +
    "[" +
    "{" +
    "\"type\": \"string\"," +
    "\"name\": \"field1\"" +
    "}" +
    "]" +
    "}\"" +
    "}"


    println(getAvroSchema)


  def getAvroSchema: String =  {
    val mapper = new ObjectMapper()

    // type object node
    val typeObjectNode = mapper.createObjectNode()
    typeObjectNode.put("type", "record")

    // name object node
    val nameObjectNode = mapper.createObjectNode()
    nameObjectNode.put("name", "confluent_kafka")

    // preparation for field object node
    val typeFieldName1 = mapper.createObjectNode()
    typeFieldName1.put("type", "int")
    val  nameFieldName1 = mapper.createObjectNode()
    nameFieldName1.put("name", "key")
    val field1 = mapper.createObjectNode()
    field1.putAll(typeFieldName1)
    field1.putAll(nameFieldName1)

    val typeFieldName2 = mapper.createObjectNode()
    typeFieldName2.put("type", "string")
    val nameFieldName2 = mapper.createObjectNode()
    nameFieldName2.put("name", "value")
    val field2 = mapper.createObjectNode()
    field2.putAll(typeFieldName2)
    field2.putAll(nameFieldName2)

    // field object node
    val fieldObjectNode = mapper.createArrayNode()
    fieldObjectNode.add(field1)
    fieldObjectNode.add(field2)

    // schema node value preparation
    val avroSchemaNode = mapper.createObjectNode()
    avroSchemaNode.putAll(typeObjectNode)
    avroSchemaNode.putAll(nameObjectNode)
    avroSchemaNode.put("fields", fieldObjectNode)

    // schema node
    val objectNode = mapper.createObjectNode()
    objectNode.put("schema", avroSchemaNode.toString)

    // save & access only Avro schema later
    schema = avroSchemaNode.toString

    mapper.createObjectNode().putAll(objectNode).toString
  }



}
