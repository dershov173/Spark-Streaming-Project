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
    val config = HDFSKafkaConnectorConfig(props)

    val fSOperationsMaintainer = initMaintainer(props)

    hdfsEventsPoller = HDFSEventsPoller(config,
      fSOperationsMaintainer,
      context,
      EventFromJsonDeserializer,
      EventIdFromFSPathConstructor())
  }

  private def initMaintainer(props: util.Map[String, String]): FSOperationsMaintainer = {
    logger.info("Trying to instantiate a FSOperationsMaintainer")
    val properties = new Properties()
    properties.putAll(props)
    FSOperationsMaintainer(PropertiesWrapper(properties))
  }

  override def poll(): util.List[SourceRecord] = {
    hdfsEventsPoller.poll()
  }

  override def stop(): Unit = {
    hdfsEventsPoller.close()
  }

  override def version(): String = VersionUtil.getVersion

}
