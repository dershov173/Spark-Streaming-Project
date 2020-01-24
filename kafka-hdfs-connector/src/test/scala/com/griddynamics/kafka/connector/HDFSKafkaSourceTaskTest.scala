package com.griddynamics.kafka.connector

import java.util

import com.griddynamics.generators.{Event, EventDeserializer, EventsGenerator, FSOperationsMaintainer}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.kafka.connect.source.SourceRecord
import org.scalacheck.Gen
import org.scalamock.handlers.{CallHandler, CallHandler2}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


class HDFSKafkaSourceTaskTest extends FlatSpec with Matchers with MockFactory {

  trait TestContext {
    val fSOperationsMaintainer: FSOperationsMaintainer = mock[FSOperationsMaintainer]
    val eventDeserializer: EventDeserializer = mock[EventDeserializer]
    val idConstructor: IdConstructor = mock[IdConstructor]
    val params: util.Map[String, String] = Map(
      "kafka.connect.topic" -> "topicName",
      "kafka.connect.events_directory" -> "events",
      "kafka.connect.defaultFS" -> "fs").asJava
    val config = new HDFSKafkaConnectorConfig(HDFSKafkaConnectorConfig.defaultConf(),
      params)

    val validPathGen: Gen[Path] = pathGenerator()
    val content: String = Gen.alphaStr.sample.get
    val event: Event = EventsGenerator().generateEvent().sample.get

    def files: Array[Path]

    def listFiles: CallHandler2[Path, PathFilter, Array[Path]] = fSOperationsMaintainer.listFiles _ expects(*, *) returning files

    def constructId(timestamp:Long, repeatsNumber: Int): CallHandler[Try[EventIdentifier]]#Derived =
      idConstructor.constructId _ expects * repeat repeatsNumber returning Success(EventIdentifier(timestamp, 0L, "", ""))

    def readFile(repeatsNumber: Int): CallHandler[Try[String]]#Derived =
      fSOperationsMaintainer.readFile _ expects(*, *, *) repeat repeatsNumber returning Success(content)

    def deserialize(repeatsNumber: Int): CallHandler[Try[Event]]#Derived =
      eventDeserializer.deserialize _ expects * repeat repeatsNumber returning Success(event)


    def pathGenerator(): Gen[Path] = for {
      timestamp <- Gen.posNum[Long]
      internalId <- Gen.chooseNum(0L, Long.MaxValue)
    } yield {
      new Path(s"${timestamp}_$internalId")
    }
  }


  "task" should "return no source records for empty events list" in new TestContext {
    override def files = Array.empty[Path]
    listFiles

    val task = new HDFSKafkaSourceTask(config,
      fSOperationsMaintainer,
      0L,
      eventDeserializer,
      idConstructor)
    task.poll() should be(empty)
  }

  "task" should "poll all events when nextFileSince is 0 successfully" in new TestContext {
    val latestEventTimestamp: Long = Gen.posNum[Long].sample.get
    val eventsNumber: Int = Gen.posNum[Int].sample.get
    override def files: Array[Path] = Gen.listOfN(eventsNumber, pathGenerator()).sample.get.toArray

    listFiles
    constructId(latestEventTimestamp, eventsNumber)
    readFile(eventsNumber)
    deserialize(eventsNumber)

    val task = new HDFSKafkaSourceTask(config,
      fSOperationsMaintainer,
      0L,
      eventDeserializer,
      idConstructor)
    val sourceRecords: util.List[SourceRecord] = task.poll()
    sourceRecords.size() should be(eventsNumber)
    sourceRecords
      .asScala
      .map(sourceRecord => Option(sourceRecord.sourceOffset()
        .get(Schemas.LAST_READ_FILE_FIELD)
        .toString
        .toLong)
        .getOrElse(0L))
      .max should be(latestEventTimestamp)
  }

  "task" should "properly handle invalid file names" in new TestContext {
    val latestEventTimestamp: Long = Gen.posNum[Long].sample.get
    val validFilesNumber: Int = Gen.posNum[Int].sample.get
    val invalidFileNamesNumber = 2
    val invalidFileNamesGen: Gen[Path] = Gen.alphaStr.map(new Path(_))
    val eventsNumber: Int = invalidFileNamesNumber + validFilesNumber


    val validFiles: Array[Path] = Gen.listOfN(validFilesNumber, validPathGen).sample.get.toArray
    val invalidFileNames: Array[Path] = Gen.listOfN(invalidFileNamesNumber, invalidFileNamesGen).sample.get.toArray

    override val files: Array[Path] = validFiles ++ invalidFileNames

    override def readFile(value: Int): CallHandler[Try[String]]#Derived = {
      fSOperationsMaintainer.readFile _ expects where {
        (p: Path, _, _) => validFiles.contains(p)
      } repeat validFilesNumber returning Success(content)
      fSOperationsMaintainer.readFile _ expects where {
        (p: Path, _, _) => invalidFileNames.contains(p)
      } repeat invalidFileNamesNumber returning Failure(new Exception())
    }

    listFiles
    constructId(latestEventTimestamp, eventsNumber)
    readFile(0)
    deserialize(validFilesNumber)

    val task = new HDFSKafkaSourceTask(config, fSOperationsMaintainer, 0L, eventDeserializer, idConstructor)
    val sourceRecords: util.List[SourceRecord] = task.poll()
    sourceRecords.size() should be(validFilesNumber)
    sourceRecords
      .asScala
      .map(sourceRecord => Option(sourceRecord.sourceOffset()
        .get(Schemas.LAST_READ_FILE_FIELD)
        .toString
        .toLong)
        .getOrElse(0L))
      .max should be(latestEventTimestamp)

  }

  "task" should "properly handle valid files containing corrupted content" in new TestContext {
    val latestEventTimestamp: Long = Gen.posNum[Long].sample.get
    val validFilesNumber: Int = Gen.posNum[Int].sample.get
    val corruptedContentFilesNumber = 2
    val corruptedContentFilesGen: Gen[Path] = Gen.alphaStr.map(new Path(_))
    val eventsNumber: Int = corruptedContentFilesNumber + validFilesNumber


    val validFiles: Array[Path] = Gen.listOfN(validFilesNumber, validPathGen).sample.get.toArray
    val corruptedContentFiles: Array[Path] = Gen.listOfN(corruptedContentFilesNumber, corruptedContentFilesGen).sample.get.toArray

    override val files: Array[Path] = validFiles ++ corruptedContentFiles

    override def constructId(timestamp: Long, repeatsNumber: Int): CallHandler[Try[EventIdentifier]]#Derived = {
      idConstructor.constructId _ expects where {
        (p:Path) => validFiles.contains(p)
      } repeat validFilesNumber returning Success(EventIdentifier(timestamp, 0L, "", ""))
      idConstructor.constructId _ expects where {
        p: Path => corruptedContentFiles.contains(p)
      } repeat corruptedContentFilesNumber returning Failure(new Exception)
    }


    listFiles
    constructId(latestEventTimestamp, eventsNumber)
    readFile(eventsNumber)
    deserialize(validFilesNumber)

    val task = new HDFSKafkaSourceTask(config, fSOperationsMaintainer, 0L, eventDeserializer, idConstructor)
    val sourceRecords: util.List[SourceRecord] = task.poll()
    sourceRecords.size() should be(validFilesNumber)
    sourceRecords
      .asScala
      .map(sourceRecord => Option(sourceRecord.sourceOffset()
        .get(Schemas.LAST_READ_FILE_FIELD)
        .toString
        .toLong)
        .getOrElse(0L))
      .max should be(latestEventTimestamp)

  }


}
