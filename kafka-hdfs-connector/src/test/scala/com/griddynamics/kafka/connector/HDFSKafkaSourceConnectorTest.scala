package com.griddynamics.kafka.connector

import java.util.Properties

import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._


class HDFSKafkaSourceConnectorTest extends FlatSpec with Matchers with MockFactory {
  "taskClass" should "be HDFSKafkaSourceTask" in {
    val taskClass = new HDFSKafkaSourceConnector().taskClass()
    assert(taskClass == classOf[HDFSKafkaSourceTask])
  }

  "connector" should "properly init task configs" in {
    val maxTasks = Gen.oneOf(1, 2, 10, 20).sample.get
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("connector.properties"))
    val map: java.util.Map[String, String] = properties
      .entrySet()
      .asScala
      .map(entry => (entry.getKey.toString, entry.getValue.toString))
      .toMap
      .asJava

    val kafkaConnectorConfig = new HDFSKafkaConnectorConfig(
      HDFSKafkaConnectorConfig.defaultConf(),
      map)

    val sourceConnector = new HDFSKafkaSourceConnector()
    sourceConnector.hdfsKafkaConnectorConfig = kafkaConnectorConfig

    val configsList = sourceConnector.taskConfigs(maxTasks)
    configsList.size() should be (maxTasks)
    configsList.asScala.distinct.asJava should be (configsList)
    configsList
      .asScala
      .map(_.get("task.id"))
      .distinct
      .size should be (maxTasks)
  }

}
