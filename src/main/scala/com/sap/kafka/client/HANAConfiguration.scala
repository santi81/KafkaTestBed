package com.sap.kafka.client

/**
 * Contains the configuration for the HANA Datasource
 *
 * Parameters in the Spark/SQLContexte prefixed with $CONF_PREFIX are always overwritten
 * by parameters in the options part
 *
 */

import com.sap.kafka.client


object HANAConfiguration {

   def prepareConfiguration(parameters: Map[String,String]): HANAConfiguration = {

    // parameters always override environment confs
    val finalParameters = parameters

    HANAConfiguration(finalParameters.get(client.PARAMETER_HOST).get,
      finalParameters.get(client.PARAMETER_INSTANCE_ID).get,
      finalParameters.get(client.PARAMETER_PORT).get,
      finalParameters.get(client.PARAMETER_USER).get,
      finalParameters.get(client.PARAMETER_PASSWORD).get)
  }

  /**
   * Returns default HANA connection port (the default port differs between
   * the multi-tenant and single-container HANA instances).
   *
   * @param parameters The user-provided parameters
   * @return The default port suffix value
   */
   def getDefaultPort(parameters: Map[String,String]): String =
    if (parameters.contains(PARAMETER_TENANT_DATABASE)) {
      DEFAULT_MULTI_CONT_HANA_PORT
    } else {
      DEFAULT_SINGLE_CONT_HANA_PORT
    }

}

case class HANAConfiguration(host: String,
                             instance: String,
                             port: String,
                             user: String,
                             password: String,
                             tenantDB: Option[String] = None)
