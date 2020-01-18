package com.griddynamics.kafka.connector

import com.griddynamics.generators.FSOperationsMaintainer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, FunSuite, Matchers}

import scala.collection.JavaConverters._


class HDFSKafkaSourceTaskTest extends FlatSpec with Matchers with MockFactory {
  "task" should "return no source records for empty events list" in {
    val fSOperationsMaintainer = mock[FSOperationsMaintainer]
    val params = Map("kafka.connect.topic" -> "topicName",
      "kafka.connect.events_directory" -> "events").asJava
    val config = new HDFSKafkaConnectorConfig(HDFSKafkaConnectorConfig.defaultConf(),
      params)

    fSOperationsMaintainer.listFiles _ expects(*, *) returning Array.empty

    val task = new HDFSKafkaSourceTask(config, fSOperationsMaintainer, null)
    task.poll() should be (empty)
  }

}
