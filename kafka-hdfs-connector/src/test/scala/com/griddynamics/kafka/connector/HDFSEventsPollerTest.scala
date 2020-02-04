package com.griddynamics.kafka.connector

import java.util

import com.griddynamics.generators.{Event, EventDeserializer, EventsGenerator, FSOperationsMaintainer}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.kafka.connect.source.SourceRecord
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalamock.handlers.{CallHandler, CallHandler2}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Assertion, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class HDFSEventsPollerTest extends FlatSpec with Matchers with MockFactory {

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

    val latestFileInternalId: Long = Gen.posNum[Long].sample.get
    val latestFileGeneratedTimestamp: Long = Gen.posNum[Long].sample.get
    val eventsNumber: Int = 0
    val validFilesNumber: Int = 0

    val poller: HDFSEventsPoller = HDFSEventsPoller(config,
      fSOperationsMaintainer,
      FilterParams(0L, 0L, 1, 1),
      eventDeserializer,
      idConstructor)

    def files: Array[Path]

    def listFiles: CallHandler2[Path, PathFilter, Array[Path]] = fSOperationsMaintainer.listFiles _ expects(*, *) returning files

    def constructId(id: Long, timestamp: Long, repeatsNumber: Int): CallHandler[Try[EventIdentifier]]#Derived =
      idConstructor.constructId _ expects * repeat repeatsNumber returning Success(EventIdentifier(timestamp, id, "", ""))

    def readFile(repeatsNumber: Int): CallHandler[Try[String]]#Derived =
      fSOperationsMaintainer.readFile _ expects * repeat repeatsNumber returning Success(content)

    def deserialize(repeatsNumber: Int): CallHandler[Try[Event]]#Derived =
      eventDeserializer.deserialize _ expects * repeat repeatsNumber returning Success(event)

    def mockAction(): CallHandler[Try[Event]]#Derived = {
      listFiles
      constructId(latestFileInternalId, latestFileGeneratedTimestamp, eventsNumber)
      readFile(eventsNumber)
      deserialize(validFilesNumber)
    }

    def pathGenerator(): Gen[Path] = for {
      timestamp <- Gen.posNum[Long]
      internalId <- Gen.chooseNum(0L, Long.MaxValue)
    } yield {
      new Path(s"${timestamp}_$internalId")
    }

    def verify(sourceRecords: util.List[SourceRecord]): Assertion = {
      sourceRecords.size() should be(validFilesNumber)

      sourceRecords
        .asScala
        .map(sourceRecord => Option(sourceRecord.sourceOffset()
          .get(Schemas.LAST_READ_FILE_INTERNAL_ID_FIELD)
          .toString
          .toLong)
          .getOrElse(0L))
        .max should be(latestFileInternalId)
    }
  }

  "poller" should "return no source records for empty events list" in new TestContext {
    override def files = Array.empty[Path]

    listFiles

    poller.poll() should be(empty)
  }

  "poller" should "poll all events when nextFileSince is 0 successfully" in new TestContext {
    override val eventsNumber: Int = Gen.posNum[Int].sample.get
    override val validFilesNumber: Int = eventsNumber

    override def files: Array[Path] = Gen.listOfN(eventsNumber, pathGenerator()).sample.get.toArray

    mockAction()

    val sourceRecords: util.List[SourceRecord] = poller.poll()

  }

  "poller" should "properly handle invalid file names" in new TestContext {
    override val validFilesNumber: Int = Gen.posNum[Int].sample.get
    val invalidFileNamesNumber = 2
    val invalidFileNamesGen: Gen[Path] = arbitrary[String].suchThat(!_.isEmpty).map(new Path(_))
    override val eventsNumber: Int = invalidFileNamesNumber + validFilesNumber


    val validFiles: Array[Path] = Gen.listOfN(validFilesNumber, validPathGen).sample.get.toArray
    val invalidFileNames: Array[Path] = Gen.listOfN(invalidFileNamesNumber, invalidFileNamesGen).sample.get.toArray

    override val files: Array[Path] = validFiles ++ invalidFileNames

    override def readFile(value: Int): CallHandler[Try[String]]#Derived = {
      fSOperationsMaintainer.readFile _ expects where {
        p: Path => validFiles.contains(p)
      } repeat validFilesNumber returning Success(content)
      fSOperationsMaintainer.readFile _ expects where {
        p: Path => invalidFileNames.contains(p)
      } repeat invalidFileNamesNumber returning Failure(new Exception())
    }

    mockAction()

    val sourceRecords: util.List[SourceRecord] = poller.poll()

    verify(sourceRecords)
  }

  "poller" should "properly handle valid files containing corrupted content" in new TestContext {
    override val validFilesNumber: Int = Gen.posNum[Int].sample.get
    val corruptedContentFilesNumber = 2
    val corruptedContentFilesGen: Gen[Path] = Gen.alphaStr.map(new Path(_))
    override val eventsNumber: Int = corruptedContentFilesNumber + validFilesNumber

    val validFiles: Array[Path] = Gen.listOfN(validFilesNumber, validPathGen).sample.get.toArray
    val corruptedContentFiles: Array[Path] = Gen.listOfN(corruptedContentFilesNumber, corruptedContentFilesGen).sample.get.toArray

    override val files: Array[Path] = validFiles ++ corruptedContentFiles

    override def constructId(id: Long, timestamp: Long, repeatsNumber: Int): CallHandler[Try[EventIdentifier]]#Derived = {
      idConstructor.constructId _ expects where {
        (p: Path) => validFiles.contains(p)
      } repeat validFilesNumber returning Success(EventIdentifier(timestamp, id, "", ""))
      idConstructor.constructId _ expects where {
        p: Path => corruptedContentFiles.contains(p)
      } repeat corruptedContentFilesNumber returning Failure(new Exception)
    }

    mockAction()

    val sourceRecords: util.List[SourceRecord] = poller.poll()

    verify(sourceRecords)

  }

}
