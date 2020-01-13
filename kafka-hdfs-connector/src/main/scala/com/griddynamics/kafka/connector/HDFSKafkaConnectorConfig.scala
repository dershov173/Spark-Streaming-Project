package com.griddynamics.kafka.connector

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

import scala.collection.JavaConverters._

object HDFSKafkaConnectorConfig {
  private val TOPIC_CONFIG = "kafka.connect.topic"
  private val TOPIC_DOC = "Topic to write to"
  private val BATCH_SIZE_CONFIG = "kafka.connect.batch.size"
  private val BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 100 (max value)"

  def apply(parsedConfig: Map[String, String]): HDFSKafkaConnectorConfig = HDFSKafkaConnectorConfig(defaultConf(), parsedConfig.asJava)

  def apply(parsedConfig: java.util.Map[String, String]): HDFSKafkaConnectorConfig = HDFSKafkaConnectorConfig(defaultConf(), parsedConfig)

  def defaultConf(): ConfigDef = {
    new ConfigDef()
      .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
      .define(BATCH_SIZE_CONFIG, Type.INT, 100, new BatchSizeValidator(), Importance.LOW, BATCH_SIZE_DOC)
  }
}

case class HDFSKafkaConnectorConfig(config: ConfigDef,
                                    parsedConfig: java.util.Map[String, String]) extends AbstractConfig(config, parsedConfig) {

  import com.griddynamics.kafka.connector.HDFSKafkaConnectorConfig._

  def getTopic: String = this.getString(TOPIC_CONFIG)

  def getBatchSIze: String = this.getString(BATCH_SIZE_CONFIG)

}

class BatchSizeValidator extends ConfigDef.Validator {
  override def ensureValid(name: String, value: Any): Unit = {
    val batchSize = value.asInstanceOf[Integer]
    if (!(1 <= batchSize && batchSize <= 100))
      throw new Exception("Batch Size must be a positive integer that's less or equal to 100")
  }
}
