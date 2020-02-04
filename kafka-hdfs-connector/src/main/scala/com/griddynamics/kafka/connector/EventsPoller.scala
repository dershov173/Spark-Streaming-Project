package com.griddynamics.kafka.connector

import java.util
import java.util.concurrent.atomic.AtomicLong

import com.griddynamics.generators.{Event, EventDeserializer, FSOperationsMaintainer}
import com.griddynamics.kafka.connector.HDFSEventsPoller.{constructRecordKey, constructRecordValue, sourcePartition}
import com.griddynamics.kafka.connector.Schemas._
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait EventsPoller {
  def poll(): java.util.List[SourceRecord]
}

object HDFSEventsPoller {
  private[connector] def sourcePartition(connectorConfig: HDFSKafkaConnectorConfig): util.Map[String, String] = {
    Map(DEFAULT_FS -> connectorConfig.getDefaultFS,
      EVENTS_DIRECTORY -> connectorConfig.getEventsDirectory).asJava
  }

  private[connector] def constructRecordKey(originalFileName: String): Struct = {
    new Struct(Schemas.KEY_SCHEMA)
      .put(Schemas.FILE_NAME_FIELD, originalFileName)
  }

  private[connector] def constructRecordValue(event: Event): Struct = {
    new Struct(VALUE_SCHEMA)
      .put(Schemas.EVENT_TYPE_FIELD, event.eventType)
      .put(Schemas.EVENT_TIME_FIELD, event.eventTime)
      .put(Schemas.IP_ADDRESS_FIELD, event.ipAddress)
      .put(Schemas.URL_FIELD, event.url)
  }
}


case class HDFSEventsPoller(config: HDFSKafkaConnectorConfig,
                            fsOperationsMaintainer: FSOperationsMaintainer,
                            initialFilterParams: FilterParams,
                            deserializer: EventDeserializer,
                            idConstructor: IdConstructor) extends AutoCloseable with EventsPoller {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val nextFileInternalId: AtomicLong = new AtomicLong(initialFilterParams.lastUploadedFileInternalId)
  private val nextFileGeneratedTimestamp: AtomicLong = new AtomicLong(initialFilterParams.generatedTimestamp)

  implicit class RicherTry[+T](wrapped: Try[T]) {
    def zip[That](that: => Try[That]): Try[(T, That)] =
      for (a <- wrapped; b <- that) yield (a, b)
  }

  override def poll(): util.List[SourceRecord] = {
    logger.info(s"Connector starts polling events from HDFS directory = {} to Kafka" +
      s"with lastReadFileTimestamp = {} and lastReadFileInternalId = {}",
      config.getEventsDirectory,
      nextFileGeneratedTimestamp.get().toString,
      nextFileInternalId.get().toString
    )
    val eventsDirectory = new Path(config.getEventsDirectory)

    val updatedFilterParams = FilterParams(lastUploadedFileInternalId = nextFileInternalId.get(),
      generatedTimestamp = nextFileGeneratedTimestamp.get(),
      tasksNumber = initialFilterParams.tasksNumber,
      taskId = initialFilterParams.taskId)

    fsOperationsMaintainer
      .listFiles(eventsDirectory,
        PathFilters.getPathFilter(updatedFilterParams))
      .toList
      .map(tryToConstructSourceRecord)
      .filter {
        case Success(event) =>
          logger.info("Event {} successfully proceeded", event.toString)
          true
        case Failure(exception) =>
          logger.error("There was an exception occurred", exception)
          false
      }
      .map(_.get)
      .asJava
  }

  private def tryToConstructSourceRecord(path: Path): Try[SourceRecord] = {
    val triedEventIdentifier = idConstructor
      .constructId(path)
    val triedEvent = fsOperationsMaintainer
      .readFile(path)
      .flatMap(deserializer.deserialize)

    val triedTuple = triedEventIdentifier.zip(triedEvent)
    sourceRecord(triedTuple)
  }

  private def sourceRecord(eventTupleTry: Try[(EventIdentifier, Event)]): Try[SourceRecord] = {
    eventTupleTry
      .map(eventTuple =>
        new SourceRecord(sourcePartition(config),
          sourceOffset(eventTuple._1),
          config.getTopic,
          initialFilterParams.taskId - 1,
          Schemas.KEY_SCHEMA,
          constructRecordKey(eventTuple._1.originalFileName),
          Schemas.VALUE_SCHEMA,
          constructRecordValue(eventTuple._2)))
  }

  private def sourceOffset(lastEventIdentifier: EventIdentifier): java.util.Map[String, String] = {
    if (nextFileGeneratedTimestamp.get < lastEventIdentifier.timestamp) {
      nextFileInternalId.set(lastEventIdentifier.internalId)
      nextFileGeneratedTimestamp.set(lastEventIdentifier.timestamp)
    } else {
      val latestReadFileInternalId = scala.math.max(nextFileInternalId.get, lastEventIdentifier.internalId)
      nextFileInternalId.set(latestReadFileInternalId)
    }

    Map(LAST_READ_FILE_INTERNAL_ID_FIELD -> nextFileInternalId.get.toString,
      LAST_READ_FILE_GENERATED_TIMESTAMP -> nextFileGeneratedTimestamp.get.toString).asJava
  }

  override def close(): Unit = {
    logger.info("Attempting to stop FSOperationsMaintainer")
    if (fsOperationsMaintainer != null) fsOperationsMaintainer.close()
    logger.info("FSOperationsMaintainer successfully closed")
  }

}