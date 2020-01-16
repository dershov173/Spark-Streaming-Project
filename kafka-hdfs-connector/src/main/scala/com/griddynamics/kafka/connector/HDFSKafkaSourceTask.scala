package com.griddynamics.kafka.connector

import java.util
import java.util.Properties

import com.griddynamics.generators.{FSOperationsMaintainer, PropertiesWrapper}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.{Logger, LoggerFactory}

class HDFSKafkaSourceTask extends SourceTask {

  import com.griddynamics.kafka.connector.Schemas._

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var nextFileSince = 0L

  private var config: HDFSKafkaConnectorConfig = _
  private var fsOperationsMaintainer: FSOperationsMaintainer = _

  override def start(props: util.Map[String, String]): Unit = {
    config = HDFSKafkaConnectorConfig(props)
    calculatePartitionsAndOffsets()

    initMaintainer(props)
  }

  private def initMaintainer(props: util.Map[String, String]): Unit = {
    logger.info("Trying to instantiate a FSOperationsMaintainer")
    val properties = new Properties()
    properties.putAll(props)
    fsOperationsMaintainer = FSOperationsMaintainer(PropertiesWrapper(properties))
    logger.info("FSOperationsMaintainer successfully instantiated")
  }

  private def calculatePartitionsAndOffsets(): Unit = {
    val sourceConfigs = context.offsetStorageReader().offset(sourcePartition())
    nextFileSince = Option(sourceConfigs
      .get(LAST_READ_FILE_FIELD)
      .asInstanceOf[String]
      .toLong)
      .getOrElse(nextFileSince)
  }

  override def poll(): util.List[SourceRecord] = ???

    {

//      logger.info("Connector starts polling events from HDFS to Kafka")
//      val eventsDirectory = new Path(fsOperationsMaintainer.eventsDirectoryName)
//
//
//      fsOperationsMaintainer
//        .listFiles(eventsDirectory, EventTimestampPathFilter(nextFileSince))
//        .toSeq
//        .map(st => fsOperationsMaintainer.readFile(st.getPath))

    }

  override def stop(): Unit = {
    logger.info("Attempting to stop FSOperationsMaintainer")
    if (fsOperationsMaintainer != null) fsOperationsMaintainer.close()
    logger.info("FSOperationsMaintainer successfully closed")
  }

  override def version(): String = VersionUtil.getVersion

  private def sourcePartition(): java.util.Map[String, String] = {
    new util.HashMap[String, String]()
  }

  private def sourceOffset(lastFileReadTimestamp: Long): java.util.Map[String, String] = {
    val latestFileReadTimestamp = scala.math.max(nextFileSince, lastFileReadTimestamp).toString

    import scala.collection.JavaConverters._
    Map(LAST_READ_FILE_FIELD -> latestFileReadTimestamp).asJava
  }
}
