package com.griddynamics.kafka.connector

import java.util

import com.griddynamics.generators.{EventDeserializer, EventFromJsonDeserializer, EventsGenerator, FSOperationsMaintainer}
import org.apache.hadoop.fs.Path
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.Success


class HDFSKafkaSourceTaskTest extends FlatSpec with Matchers with MockFactory {

  trait TestContext {
    val fSOperationsMaintainer: FSOperationsMaintainer = mock[FSOperationsMaintainer]
    val eventDeserializer: EventDeserializer = mock[EventDeserializer]
    val params: util.Map[String, String] = Map(
      "kafka.connect.topic" -> "topicName",
      "kafka.connect.events_directory" -> "events",
      "kafka.connect.defaultFS" -> "fs").asJava
    val config = new HDFSKafkaConnectorConfig(HDFSKafkaConnectorConfig.defaultConf(),
      params)
    var latestEventTimestamp = 0L
    def pathGenerator(timestampGen: Gen[Long]): Gen[Path] = for {
      timestamp <- timestampGen
      internalId <- Gen.chooseNum(0L, Long.MaxValue)
    } yield {
      latestEventTimestamp = math.max(latestEventTimestamp, timestamp)
      new Path(s"${timestamp}_$internalId")
    }
  }


  "task" should "return no source records for empty events list" in new TestContext {
    fSOperationsMaintainer.listFiles _ expects(*, *) returning Array.empty

    val task = new HDFSKafkaSourceTask(config, fSOperationsMaintainer, 0L, EventFromJsonDeserializer)
    task.poll() should be(empty)
    latestEventTimestamp should be(0L)
  }

  "task" should "poll all events when nextFileSince is 0" in new TestContext {
    val eventsNumber = Gen.posNum[Int].sample.get
    val files = Gen.listOfN(eventsNumber, pathGenerator(Gen.posNum[Long])).sample.get.toArray
    val content = Gen.alphaStr.sample.get
    val event = EventsGenerator().generateEvent().sample.get

    fSOperationsMaintainer.listFiles _ expects(*, *) returning files
    fSOperationsMaintainer.readFile _ expects(*, *, *) repeat eventsNumber returning Success(content)
    eventDeserializer.deserialize _ expects * repeat eventsNumber returning Success(event)

    val task = new HDFSKafkaSourceTask(config, fSOperationsMaintainer, 0L, eventDeserializer)
    task.poll().size() should be(eventsNumber)
  }


}
