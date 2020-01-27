package com.griddynamics.kafka.connector

import java.util

import com.griddynamics.generators.{Event, EventDeserializer, FSOperationsMaintainer}
import com.griddynamics.kafka.connector.Schemas.{DEFAULT_FS, EVENTS_DIRECTORY, LAST_READ_FILE_FIELD, VALUE_SCHEMA}
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.{SourceRecord, SourceTaskContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait EventsPoller {
  def poll(): java.util.List[SourceRecord]
}

case class HDFSEventsPoller(config: HDFSKafkaConnectorConfig,
                            fsOperationsMaintainer: FSOperationsMaintainer,
                            context: SourceTaskContext,
                            deserializer: EventDeserializer,
                            idConstructor: IdConstructor) extends AutoCloseable {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val nextFileSince: Long = Option(context)
    .flatMap(ctx => {
      logger.info(s"Task Context: ${ctx.toString}")
      Option(ctx.offsetStorageReader())
    })
    .flatMap(reader => {
      logger.info(s"Offset reader: ${reader.toString}")
      Option(reader.offset(sourcePartition(config)))
    })
    .flatMap(offset => {
      logger.info(s"Offset: ${offset.toString}")
      Option(offset.get(LAST_READ_FILE_FIELD))
    })
    .map(_.asInstanceOf[String].toLong)
    .getOrElse(0L)

  implicit class RicherTry[+T](wrapped: Try[T]) {
    def zip[That](that: => Try[That]): Try[(T, That)] =
      for (a <- wrapped; b <- that) yield (a, b)
  }

  def poll(): util.List[SourceRecord] = {
    logger.info(s"Connector starts polling events from HDFS directory = {} to Kafka" +
      s"with lastReadFileTimestamp = {}", config.getEventsDirectory, nextFileSince)
    val eventsDirectory = new Path(config.getEventsDirectory)

    fsOperationsMaintainer
      .listFiles(eventsDirectory, EventTimestampPathFilter(nextFileSince))
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
          sourceOffset(eventTuple._1.timestamp),
          config.getTopic,
          null,
          Schemas.KEY_SCHEMA,
          constructRecordKey(eventTuple._1.originalFileName),
          Schemas.VALUE_SCHEMA,
          constructRecordValue(eventTuple._2)))
  }


  private def sourcePartition(connectorConfig: HDFSKafkaConnectorConfig): util.Map[String, String] = {
    Map(DEFAULT_FS -> connectorConfig.getDefaultFS,
      EVENTS_DIRECTORY -> connectorConfig.getEventsDirectory).asJava
  }

  private def sourceOffset(lastFileReadTimestamp: Long): java.util.Map[String, String] = {
    val latestFileReadTimestamp = scala.math.max(nextFileSince, lastFileReadTimestamp).toString

    Map(LAST_READ_FILE_FIELD -> latestFileReadTimestamp).asJava
  }

  private def constructRecordKey(originalFileName: String): Struct = {
    new Struct(Schemas.KEY_SCHEMA)
      .put(Schemas.FILE_NAME_FIELD, originalFileName)
  }

  private def constructRecordValue(event: Event): Struct = {
    new Struct(VALUE_SCHEMA)
      .put(Schemas.EVENT_TYPE_FIELD, event.eventType)
      .put(Schemas.EVENT_TIME_FIELD, event.eventTime)
      .put(Schemas.IP_ADDRESS_FIELD, event.ipAddress)
      .put(Schemas.URL_FIELD, event.url)
  }

  override def close(): Unit = {
    logger.info("Attempting to stop FSOperationsMaintainer")
    if (fsOperationsMaintainer != null) fsOperationsMaintainer.close()
    logger.info("FSOperationsMaintainer successfully closed")
  }

}