package com.griddynamics.kafka.connector

import org.apache.hadoop.fs.{Path, PathFilter}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

case class EventIdentifier(timestamp: Long,
                           internalId: Long,
                           instanceName: String,
                           originalFileName: String) {
  def getInstanceNameOpt: Option[String] = Option(instanceName)
}


object FSPathToEventIdMapper {
  def apply(): FSPathToEventIdMapper = new FSPathToEventIdMapper("_".r)
}

case class FSPathToEventIdMapper(splitBy: Regex) {
  def map(p: Path): Try[EventIdentifier] = Try {
    val strings = splitBy.split(p.getName)
    if (strings.length < 2 || strings.length > 3) return Failure(
      new IllegalStateException(s"Processed file name is not in proper format ${p.getName} for given spliterator $splitBy"))

    val timestamp = strings(0).toLong
    val internalId = strings(1).toLong
    val instanceNameOpt =
      if (strings.length == 3)
        strings(2)
      else null
    EventIdentifier(timestamp, internalId, instanceNameOpt, p.getName)
  }
}

case class EventTimestampPathFilter(lastUploadedFileTimestamp: Long) extends PathFilter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def accept(path: Path): Boolean = {
    FSPathToEventIdMapper()
      .map(path) match {
      case Success(eventIdentifier) => eventIdentifier.timestamp > lastUploadedFileTimestamp
      case Failure(exception) =>
        logger.error(s"There was an exception occurred while trying to process file ${path}", exception)
        false
    }
  }
}
