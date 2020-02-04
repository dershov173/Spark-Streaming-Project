package com.griddynamics.kafka.connector

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object HDFSKafkaConnectorConfig {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val TOPIC_CONFIG = "kafka.connect.topic"
  private val TOPIC_DOC = "Topic to write to"
  private val BATCH_SIZE_CONFIG = "kafka.connect.batch.size"
  private val BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 100 (max value)"
  private val DEFAULT_FS_CONFIG = "kafka.connect.defaultFS"
  private val DEFAULT_FS_DOC = "Defines File System host:port to connect to"
  private val EVENTS_DIRECTORY_CONFIG = "kafka.connect.events_directory"
  private val EVENTS_DIRECTORY_DOC = "HDFS directory path to read events from"
  private val MAX_TASKS_NUMBER_CONFIG = "tasks.max"
  private val MAX_TASKS_NUMBER_DOC = "Integer number to define connector parallelism level"


  def apply(parsedConfig: Map[String, String]): HDFSKafkaConnectorConfig = HDFSKafkaConnectorConfig(defaultConf(), parsedConfig.asJava)

  def apply(parsedConfig: java.util.Map[String, String]): HDFSKafkaConnectorConfig = HDFSKafkaConnectorConfig(defaultConf(), parsedConfig)

  def defaultConf(): ConfigDef = {
    new ConfigDef()
      .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
      .define(BATCH_SIZE_CONFIG, Type.INT, 100, new BatchSizeValidator(), Importance.LOW, BATCH_SIZE_DOC)
      .define(EVENTS_DIRECTORY_CONFIG, Type.STRING, Importance.HIGH, EVENTS_DIRECTORY_DOC)
      .define(DEFAULT_FS_CONFIG, Type.STRING, Importance.HIGH, DEFAULT_FS_DOC)
      .define(MAX_TASKS_NUMBER_CONFIG, Type.INT, 1, Importance.HIGH, MAX_TASKS_NUMBER_DOC)
  }
}

case class HDFSKafkaConnectorConfig(config: ConfigDef,
                                    parsedConfig: java.util.Map[String, String]) extends AbstractConfig(config, parsedConfig) {

  import com.griddynamics.kafka.connector.HDFSKafkaConnectorConfig._

  logger.info("Following parsed config has been obtained: {}", parsedConfig.toString)

  def getTopic: String = this.getString(TOPIC_CONFIG)

  def getBatchSIze: Int = this.getInt(BATCH_SIZE_CONFIG)

  def getEventsDirectory: String = this.getString(EVENTS_DIRECTORY_CONFIG)

  def getDefaultFS: String = this.getString(DEFAULT_FS_CONFIG)

  def getMaxTasksNumber : Int = this.getInt(MAX_TASKS_NUMBER_CONFIG)

  def getCurrentTaskId : Int = this.parsedConfig.get(Schemas.CURRENT_TASK_ID).toInt

}

class BatchSizeValidator extends ConfigDef.Validator {
  override def ensureValid(name: String, value: Any): Unit = {
    val batchSize = value.asInstanceOf[Integer]
    if (!(1 <= batchSize && batchSize <= 100))
      throw new Exception("Batch Size must be a positive integer that's less or equal to 100")
  }
}
