package com.griddynamics.kafka.connector

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

class HDFSKafkaSourceConnectorTest extends FlatSpec with Matchers with MockFactory {
  "taskClass" should "be HDFSKafkaSourceTask" in {
    val taskClass = new HDFSKafkaSourceConnector().taskClass()
    assert(taskClass == classOf[HDFSKafkaSourceTask])
  }

}
