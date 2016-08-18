package com.sap.kafka.client

class HANAClientException(msg: String, cause: Throwable)
  extends RuntimeException(msg, cause)

class HANAJdbcException(config: HANAConfiguration, msg: String, cause: Throwable)
  extends HANAClientException(s"[$config] $msg", cause)

class HANAJdbcBadStateException(config: HANAConfiguration, msg: String, cause: Throwable)
  extends HANAJdbcException(config, msg, cause)

class HANAJdbcConnectionException(config: HANAConfiguration, msg: String, cause: Throwable)
  extends HANAJdbcException(config, msg, cause)
