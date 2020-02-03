package com.griddynamics.kafka.connector

import org.apache.kafka.connect.data.{Schema, SchemaBuilder}

object Schemas {
  val SCHEMA_KEY_NAME = "com.griddynamics.kafka.connector.EventKey"
  val SCHEMA_VALUE_NAME = "com.griddynamcis.kafka.connector.EventValue"

  val FILE_NAME_FIELD = "fileName"
  val EVENT_TYPE_FIELD = "eventType"
  val IP_ADDRESS_FIELD = "ipAddress"
  val EVENT_TIME_FIELD = "eventTime"
  val URL_FIELD = "url"

  val LAST_READ_FILE_INTERNAL_ID_FIELD = "lastReadFileInternalId"
  val LAST_READ_FILE_GENERATED_TIMESTAMP = "lastReadFileGeneratedTimestamp"
  val DEFAULT_FS = "defaultFS"
  val EVENTS_DIRECTORY = "eventsDirectory"

  val KEY_SCHEMA: Schema = SchemaBuilder
    .struct()
    .name(SCHEMA_KEY_NAME)
    .field(FILE_NAME_FIELD, Schema.STRING_SCHEMA)
    .build()

  val VALUE_SCHEMA: Schema = SchemaBuilder
    .struct()
    .name(SCHEMA_VALUE_NAME)
    .field(EVENT_TYPE_FIELD, Schema.STRING_SCHEMA)
    .field(IP_ADDRESS_FIELD, Schema.STRING_SCHEMA)
    .field(EVENT_TIME_FIELD, Schema.STRING_SCHEMA)
    .field(URL_FIELD, Schema.STRING_SCHEMA)
    .build()
}
