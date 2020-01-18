package com.griddynamics.kafka.connector

import java.util
import java.util.Collections

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.slf4j.{Logger, LoggerFactory}

class HDFSKafkaSourceConnector extends SourceConnector {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var hdfsKafkaConnectorConfig: HDFSKafkaConnectorConfig = _

  override def start(props: util.Map[String, String]): Unit = hdfsKafkaConnectorConfig = {
    logger.info("HDFS to Kafka connector is getting started")
    HDFSKafkaConnectorConfig(props)
  }

  override def taskClass(): Class[_ <: Task] = HDFSKafkaSourceTask.getClass

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] =
    Collections.singletonList(hdfsKafkaConnectorConfig.originalsStrings()) //todo:rework this behaviour

  override def stop(): Unit = {
    logger.info("HDFS Kafka Connector is getting stopped")
  }

  override def config(): ConfigDef = HDFSKafkaConnectorConfig.defaultConf()

  override def version(): String = VersionUtil.getVersion
}


