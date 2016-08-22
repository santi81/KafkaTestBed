
package com.sap.kafka.schema

import org.apache.kafka.connect.data.Schema

case class SchemaPair(var keySchema: Schema,var valueSchema: Schema)