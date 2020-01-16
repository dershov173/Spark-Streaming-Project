package com.griddynamics.kafka.connector

import org.apache.hadoop.fs.Path

import scala.util.matching.Regex
import scala.util.{Failure, Try}

case class EventIdentifier(timestamp: Long, internalId: Long, instanceNameOpt: Option[String])


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
        Option(strings(2))
      else None
    EventIdentifier(timestamp, internalId, instanceNameOpt)
  }
}
