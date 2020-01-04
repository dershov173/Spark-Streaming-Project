package com.griddynamics.generators

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

case class FSOperationsMaintainer(fs: FileSystem,
                                  private val prefix: String = "/events",
                                  private val extension: String = "json") extends AutoCloseable{
  override def close(): Unit = fs.close()

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val id = new AtomicLong(0)

  def generateUniquePath: Path =
    new Path(s"$prefix${System.currentTimeMillis()}_${id.getAndIncrement()}.$extension")

  def writeToHDFS(p: Path, eventJson: String): Unit = {
    val outputStream = fs.create(p, false)
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

  def mkdirs(p:Path, permission:FsPermission): Boolean = fs.mkdirs(p, permission)

  def mkdirs(p:Path): Boolean = fs.mkdirs(p)

}
