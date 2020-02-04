package com.griddynamics.kafka.connector

import java.util
import java.util.concurrent.CopyOnWriteArrayList

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.slf4j.{Logger, LoggerFactory}

class HDFSKafkaSourceConnector extends SourceConnector {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private[connector] var hdfsKafkaConnectorConfig: HDFSKafkaConnectorConfig = _

  override def start(props: util.Map[String, String]): Unit = hdfsKafkaConnectorConfig = {
    logger.info("HDFS to Kafka connector is getting started")
    HDFSKafkaConnectorConfig(props)
  }

  override def taskClass(): Class[_ <: Task] = classOf[HDFSKafkaSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    val configList = new CopyOnWriteArrayList[util.Map[String, String]]()
    for (i <- 1 to maxTasks) {
      val originalStrings = hdfsKafkaConnectorConfig.originalsStrings()
      originalStrings.put(Schemas.CURRENT_TASK_ID, i.toString)
      configList.add(originalStrings)
    }
    configList
  }

  override def stop(): Unit = {
    logger.info("HDFS Kafka Connector is getting stopped")
  }

  override def config(): ConfigDef = HDFSKafkaConnectorConfig.defaultConf()

  override def version(): String = VersionUtil.getVersion
}


