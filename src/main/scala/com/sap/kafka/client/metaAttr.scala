package com.sap.kafka.client

// It overrides a JDBC class - keep it starting with lowercase
case class metaAttr(
    name: String,
    dataType: Int,
    isNullable: Int,
    precision: Int,
    scale: Int,
    isSigned: Boolean)
