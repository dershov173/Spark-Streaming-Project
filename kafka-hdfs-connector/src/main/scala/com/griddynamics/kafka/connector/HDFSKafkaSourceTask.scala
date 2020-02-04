package com.griddynamics.kafka.connector

import java.util
import java.util.Properties

import com.griddynamics.generators._
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.{Logger, LoggerFactory}

class HDFSKafkaSourceTask extends SourceTask {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var hdfsEventsPoller: HDFSEventsPoller = _

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Starting task with config = {}", props.toString)
    val config = HDFSKafkaConnectorConfig(props)

    val fSOperationsMaintainer = initMaintainer(props)
    val initialFilterParams = initFilterParams(config)

    hdfsEventsPoller = HDFSEventsPoller(config,
      fSOperationsMaintainer,
      initialFilterParams,
      EventFromJsonDeserializer,
      EventIdFromFSPathConstructor())
  }

  private def initMaintainer(props: util.Map[String, String]): FSOperationsMaintainer = {
    logger.info("Trying to instantiate a FSOperationsMaintainer")
    val properties = new Properties()
    properties.putAll(props)
    FSOperationsMaintainer(PropertiesWrapper(properties))
  }

  private def initFilterParams(connectorConfig: HDFSKafkaConnectorConfig) : FilterParams = {
    val lastProcessedFileInternalId = getConfigValueFor(connectorConfig, Schemas.LAST_READ_FILE_INTERNAL_ID_FIELD)
    val lastProcessedFileGeneratedTimestamp = getConfigValueFor(connectorConfig, Schemas.LAST_READ_FILE_GENERATED_TIMESTAMP)
    val tasksNumber = connectorConfig.getMaxTasksNumber
    val currentTaskId = connectorConfig.getCurrentTaskId

    logger.info("The events directory will be traversed starting with timestamp = {} and internalId = {}." +
      "Tasks number = {}, current Task id = {}",
      lastProcessedFileGeneratedTimestamp.toString,
      lastProcessedFileInternalId.toString,
      tasksNumber.toString,
      currentTaskId.toString
    )

    FilterParams(lastProcessedFileInternalId,
      lastProcessedFileGeneratedTimestamp,
      tasksNumber,
      currentTaskId)
  }

  private def getConfigValueFor(config: HDFSKafkaConnectorConfig, key:String) : Long = {
    logger.info("Loading task with Context = {}", context.toString)
    val confValueOpt = for {
      contextOpt <- Option(context)
      offsetStorageReaderOpt <- Option(contextOpt.offsetStorageReader())
      configsOpt <- Option(offsetStorageReaderOpt.offset(HDFSEventsPoller.sourcePartition(config)))
      confValue <- Option(configsOpt.get(key))
    } yield confValue

    val actualConfValue = confValueOpt match {
      case Some(l: java.lang.Long) => l.longValue()
      case Some(s: String) => s.toLong
      case _ => 0L
    }
    actualConfValue
  }

  override def poll(): util.List[SourceRecord] = {
    hdfsEventsPoller.poll()
  }

  override def stop(): Unit = {
    logger.info("Attempting to stop task")
    hdfsEventsPoller.close()
    logger.info("Task has been successfully stopped")
  }

  override def version(): String = VersionUtil.getVersion

}
