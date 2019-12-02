package com.griddynamics.generators

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._


object HDFSWriter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val id = new AtomicLong(0)
  private val prefix = "/events/"
  private val extension = "json"

  def getPath(): Path = {
    new Path(s"$prefix${System.currentTimeMillis()}_${id.getAndIncrement()}.$extension")
  }
}

case class HDFSWriter(fs: FileSystem) {
  def writeEventsToHDFS(eventJson: String): Unit = {
    import com.griddynamics.generators.HDFSWriter._
    val pathName = getPath()
    val outputStream = fs.create(pathName, false)

    try {
      outputStream.writeChars(eventJson)
      outputStream.flush()

      logger.info("event flushed to hdfs {}", eventJson)
    } catch {
      case t: Throwable => logger.error("There was an exception occurred while attempting to flush event to HDFS",
        t)
    } finally {
      outputStream.close()
    }
  }
}

object EventToJsonSerializer {
  def eventToJson(event: Event): String = {
    implicit val eventWrites: Writes[Event] = new Writes[Event] {
      def writes(event: Event): JsValue = Json.obj(
        "eventType" -> event.eventType,
        "ipAddress" -> event.ipAddress,
        "eventTime" -> event.eventTime,
        "url" -> event.url
      )
    }
    Json.toJson(event).toString()
  }
}
