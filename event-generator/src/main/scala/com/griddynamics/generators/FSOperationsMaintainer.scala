package com.griddynamics.generators

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object FSOperationsMaintainer {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val defaultBufferSize: Int = 4096
  private val defaultEncoding: String = "UTF-8"

  private val defaultFSConfig = "fs.defaultFS"
  private val useDatanodeHostnameConfig = "dfs.client.use.datanode.hostname"
  private val fsEventsDirectoryConfig = "fs.events.directory"
  private val extensionConfig = "fs.outputFiles.extension"

  def apply(propertiesWrapper: PropertiesWrapper): FSOperationsMaintainer = {
    val defaultFS = propertiesWrapper.getProperty(defaultFSConfig)
    if (defaultFS == null) throw
      new IllegalArgumentException("There is no required config fs.defaultFS set ")
    val useDatanodeHostname = propertiesWrapper
      .getBooleanProperty(useDatanodeHostnameConfig, true)
    val eventsDirectoryName = propertiesWrapper
      .getOrDefaultString(fsEventsDirectoryConfig, "/events")
    val extension = propertiesWrapper
      .getOrDefaultString(extensionConfig, "json")

    val configuration = new Configuration()
    configuration.set(defaultFSConfig, defaultFS)
    configuration.set(useDatanodeHostnameConfig, useDatanodeHostname.toString)

    val fileSystem = FileSystem.get(configuration)

    FSOperationsMaintainer(fileSystem, eventsDirectoryName, extension)
  }
}

case class FSOperationsMaintainer(fs: FileSystem,
                                  eventsDirectoryName: String,
                                  private val extension: String) extends AutoCloseable {

  import com.griddynamics.generators.FSOperationsMaintainer._

  override def close(): Unit = fs.close()


  private val id = new AtomicLong(0)

  def generateUniquePath: Path =
    new Path(s"$eventsDirectoryName/${id.getAndIncrement()}_${System.currentTimeMillis()}.$extension")

  def writeToHDFS(p: Path, eventJson: String): Try[Unit] = Try {
    val outputStream = fs.create(p, false)
    try {
      outputStream.writeChars(eventJson)
      outputStream.flush()

      logger.info("event flushed to hdfs {}", eventJson)
    } catch {
      case t: Throwable =>
        logger.error("There was an exception occurred" +
          " while attempting to flush event to HDFS", t)
    } finally {
      outputStream.close()
    }
  }

  def listFiles(p: Path, filter: PathFilter): Array[FileStatus] = fs.listStatus(p, filter)

  def mkdirs(p: Path, permission: FsPermission): Boolean = fs.mkdirs(p, permission)

  def mkdirs(p: Path): Boolean = fs.mkdirs(p)

  def readFile(p: Path, bufferSize: Int = defaultBufferSize, encoding: String = defaultEncoding): String = {
    val byteBuffer = new Array[Byte](bufferSize)
    val byteArrayOutputStream = new ByteArrayOutputStream()

    val inputStream = fs.open(p)
    var readCount:Int = 0

    def readBytes(): Boolean = {
      readCount = inputStream.read(byteBuffer)
      readCount > -1
    }
    while (readBytes()) {
      byteArrayOutputStream.write(byteBuffer, 0, readCount)
    }
    byteArrayOutputStream.toString(encoding)
  }

}
