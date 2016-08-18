package com.sap.kafka.client

/**
 * The [[AbstractHANAPartitionLoader]] which uses [[HANAJdbcClient]].
 */
object HANAPartitionLoader extends AbstractHANAPartitionLoader {

  /** @inheritdoc */
  def getHANAJdbcClient(hanaConfiguration: HANAConfiguration): HANAJdbcClient =
    new HANAJdbcClient(hanaConfiguration)

}
