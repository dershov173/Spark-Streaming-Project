package com.griddynamics.kafka.connector

import java.util
import java.util.Properties

import com.griddynamics.generators.{FSOperationsMaintainer, PropertiesWrapper}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.{Logger, LoggerFactory}

class HDFSKafkaSourceTask extends SourceTask {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var config: HDFSKafkaConnectorConfig = _
  private var fsOperationsMaintainer: FSOperationsMaintainer = _

  override def start(props: util.Map[String, String]): Unit = {
    config = HDFSKafkaConnectorConfig(props)

    logger.info("Trying to instantiate a FSOperationsMaintainer")
    val properties = new Properties()
    properties.putAll(props)
    fsOperationsMaintainer = FSOperationsMaintainer(PropertiesWrapper(properties))
    logger.info("FSOperationsMaintainer successfully instantiated")
  }

  override def poll(): util.List[SourceRecord] = ???
//  {
//
//    logger.info("Connector starts polling events from HDFS to Kafka")
//    val eventsDirectory = new Path(fsOperationsMaintainer.eventsDirectoryName)
//
//
//    fsOperationsMaintainer
//      .listFiles(eventsDirectory, new PathFilter {
//      override def accept(path: Path): Boolean = true
//    })
//      .toSeq
//      .map(st => fsOperationsMaintainer.readFile(st.getPath).toString("UTF-8"))
//
//  }

  override def stop(): Unit = {
    logger.info("Attempting to stop FSOperationsMaintainer")
    if (fsOperationsMaintainer != null) fsOperationsMaintainer.close()
    logger.info("FSOperationsMaintainer successfully closed")
  }

  override def version(): String = VersionUtil.getVersion
}
