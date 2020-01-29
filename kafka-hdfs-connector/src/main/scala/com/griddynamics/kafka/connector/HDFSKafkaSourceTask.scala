package com.griddynamics.kafka.connector

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

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
    val nextFileSince = getNextFileSinceTimestamp(config)

    hdfsEventsPoller = HDFSEventsPoller(config,
      fSOperationsMaintainer,
      new AtomicLong(nextFileSince),
      EventFromJsonDeserializer,
      EventIdFromFSPathConstructor())
  }

  private def initMaintainer(props: util.Map[String, String]): FSOperationsMaintainer = {
    logger.info("Trying to instantiate a FSOperationsMaintainer")
    val properties = new Properties()
    properties.putAll(props)
    FSOperationsMaintainer(PropertiesWrapper(properties))
  }

  private def getNextFileSinceTimestamp(config: HDFSKafkaConnectorConfig) : Long = {
    logger.info("Loading task with Context = {}", context.toString)
    val nextFileSinceOpt = for {
      contextOpt <- Option(context)
      offsetStorageReaderOpt <- Option(contextOpt.offsetStorageReader())
      configsOpt <- Option(offsetStorageReaderOpt.offset(HDFSEventsPoller.sourcePartition(config)))
      lastReadFileTmstOpt <- Option(configsOpt.get(Schemas.LAST_READ_FILE_FIELD))
    } yield lastReadFileTmstOpt

    val nextFileSince = nextFileSinceOpt match {
      case Some(l: java.lang.Long) => l.longValue()
      case Some(s: String) => s.toLong
      case _ => 0L
    }
    logger.info("The events directory will be traversed starting with timestamp = {}", nextFileSince)
    nextFileSince
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
