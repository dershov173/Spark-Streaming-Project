package com.griddynamics.kafka.connector

import org.apache.hadoop.fs.Path

import scala.util.matching.Regex
import scala.util.{Failure, Try}

trait IdConstructor {
  def constructId(p: Path): Try[EventIdentifier]
}

case class EventIdentifier(timestamp: Long,
                           internalId: Long,
                           instanceName: String,
                           originalFileName: String) {
  def getInstanceNameOpt: Option[String] = Option(instanceName)
}


object EventIdFromFSPathConstructor {
  private val timestampIdx = 0
  private val internalIdIdx = 1
  private val instanceNameIdx = 2
  private val minDelimitedFileNameLength: Int = 3
  private val maxDelimitedFileNameLength: Int = 4

  def apply(): EventIdFromFSPathConstructor = new EventIdFromFSPathConstructor("[._]".r)
}

case class EventIdFromFSPathConstructor(splitBy: Regex) extends IdConstructor {

  import com.griddynamics.kafka.connector.EventIdFromFSPathConstructor._

  override def constructId(p: Path): Try[EventIdentifier] = Try {
    val strings = splitBy.split(p.getName)
    if (strings.length < minDelimitedFileNameLength || strings.length > maxDelimitedFileNameLength) return Failure(
      new IllegalStateException(s"Processed file name is not in proper format ${p.getName} for given spliterator $splitBy"))

    val timestamp = strings(timestampIdx).toLong
    val internalId = strings(internalIdIdx).toLong
    val instanceNameOpt =
      if (strings.length == maxDelimitedFileNameLength)
        strings(instanceNameIdx)
      else null
    EventIdentifier(timestamp, internalId, instanceNameOpt, p.getName)
  }
}
