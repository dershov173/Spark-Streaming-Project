package com.griddynamics.generators

import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._


case class HDFSWriter(fs:FileSystem) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def writeEventsToHDFS(eventWithTimeCreated: (String, Long)): Unit = {
    val pathName = new Path(s"/events/${eventWithTimeCreated._2}.json")

    val outputStream = fs.create(pathName, false)

    outputStream.writeChars(eventWithTimeCreated._1)
    outputStream.flush()

    logger.info("event flushed to hdfs {}", eventWithTimeCreated._1)
    outputStream.close()

  }
}

object EventToJsonSerializer {
  def eventToJson(event: Event): (String, Long) = {
    implicit val eventWrites: Writes[Event] = new Writes[Event] {
      def writes(event: Event): JsValue = Json.obj(
        "eventType" -> event.eventType,
        "ipAddress" -> event.ipAddress,
        "eventTime" -> event.eventTime,
        "url" -> event.url
      )
    }
    (Json.toJson(event).toString(),System.currentTimeMillis())
  }
}
