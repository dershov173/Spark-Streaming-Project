package com.griddynamics.kafka.connector

import java.util
import java.util.Properties

import com.griddynamics.generators.{Event, EventFromJsonDeserializer, FSOperationsMaintainer, PropertiesWrapper}
import com.griddynamics.kafka.connector.HDFSKafkaSourceTask.logger
import com.griddynamics.kafka.connector.Schemas.{LAST_READ_FILE_FIELD, VALUE_SCHEMA}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.{SourceRecord, SourceTask, SourceTaskContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object HDFSKafkaSourceTask extends SourceTask {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var hdfsKafkaSourceTask: HDFSKafkaSourceTask = _

  override def start(props: util.Map[String, String]): Unit = {
    val config = HDFSKafkaConnectorConfig(props)

    val fSOperationsMaintainer = initMaintainer(props)

    hdfsKafkaSourceTask = HDFSKafkaSourceTask(config, fSOperationsMaintainer, context)
  }

  private def initMaintainer(props: util.Map[String, String]): FSOperationsMaintainer = {
    logger.info("Trying to instantiate a FSOperationsMaintainer")
    val properties = new Properties()
    properties.putAll(props)
    FSOperationsMaintainer(PropertiesWrapper(properties))
  }

  override def poll(): util.List[SourceRecord] = {
    hdfsKafkaSourceTask.poll()
  }

  override def stop(): Unit = {
    hdfsKafkaSourceTask.close()
  }

  override def version(): String = VersionUtil.getVersion

}

private[connector] case class HDFSKafkaSourceTask(config: HDFSKafkaConnectorConfig,
                                                  fsOperationsMaintainer: FSOperationsMaintainer,
                                                  context: SourceTaskContext) extends AutoCloseable {
  val nextFileSince: Long = Option(context)
    .map(_
      .offsetStorageReader()
      .offset(sourcePartition())
      .get(LAST_READ_FILE_FIELD)
      .asInstanceOf[String]
      .toLong)
    .getOrElse(0L)

  implicit class RicherTry[+T](wrapped: Try[T]) {
    def zip[That](that: => Try[That]): Try[(T, That)] =
      for (a <- wrapped; b <- that) yield (a, b)
  }

  def poll(): util.List[SourceRecord] = {
    logger.info("Connector starts polling events from HDFS to Kafka")
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

  private def tryToConstructSourceRecord(fileStatus: FileStatus): Try[SourceRecord] = {
    val triedEventIdentifier = FSPathToEventIdMapper()
      .map(fileStatus.getPath)
    val triedEvent = fsOperationsMaintainer
      .readFile(fileStatus.getPath)
      .flatMap(EventFromJsonDeserializer.eventFromJson)

    val triedTuple = triedEventIdentifier.zip(triedEvent)
    sourceRecord(triedTuple)
  }

  private def sourceRecord(eventTupleTry: Try[(EventIdentifier, Event)]): Try[SourceRecord] = {
    eventTupleTry
      .map(eventTuple =>
        new SourceRecord(sourcePartition(),
          sourceOffset(eventTuple._1.timestamp),
          config.getTopic,
          null,
          Schemas.KEY_SCHEMA,
          constructRecordKey(eventTuple._1.originalFileName),
          Schemas.VALUE_SCHEMA,
          constructRecordValue(eventTuple._2)))
  }

  private def sourcePartition(): java.util.Map[String, String] = {
    new util.HashMap[String, String]()
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
