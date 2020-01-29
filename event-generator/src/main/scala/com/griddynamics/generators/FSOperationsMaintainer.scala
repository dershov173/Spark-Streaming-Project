package com.griddynamics.generators

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path, PathFilter}
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
      .getBooleanProperty(useDatanodeHostnameConfig, false)
    val eventsDirectoryName = propertiesWrapper
      .getOrDefaultString(fsEventsDirectoryConfig, "/events")
    val extension = propertiesWrapper
      .getOrDefaultString(extensionConfig, "json")

    val configuration = new Configuration()
    configuration.set(defaultFSConfig, defaultFS)
    configuration.set(useDatanodeHostnameConfig, useDatanodeHostname.toString)
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    val fileSystem = FileSystem.get(configuration)

    FSOperationsMaintainer(fileSystem, eventsDirectoryName, extension)
  }
}

case class FSOperationsMaintainer(fs: FileSystem,
                                  eventsDirectoryName: String = "/events",
                                  private val extension: String = "json") extends AutoCloseable {

  import com.griddynamics.generators.FSOperationsMaintainer._

  def create(p: Path, overwrite: Boolean): CustomFSDataOutputStream = CustomFSDataOutputStream(fs.create(p, overwrite))

  override def close(): Unit = fs.close()


  private val id = new AtomicLong(0)

  def generateUniquePath: Path =
    new Path(s"$eventsDirectoryName/${System.currentTimeMillis()}_${id.getAndIncrement()}.$extension")

  def writeToHDFS(p: Path, eventJson: String): Try[Unit] = Try {
    val outputStream = this.create(p, false)
    try {
      outputStream.writeUTF(eventJson)
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

  def listFiles(p: Path, filter: PathFilter): Array[Path] = fs
    .listStatus(p, filter)
    .map(_.getPath)

  def mkdirs(p: Path, permission: FsPermission): Boolean = fs.mkdirs(p, permission)

  def mkdirs(p: Path): Boolean = fs.mkdirs(p)

  def readFile(p: Path): Try[String] = Try {
    var inputStream: FSDataInputStream = null
    try {
      inputStream = fs.open(p)
      inputStream.readUTF()
    } finally {
      if (inputStream != null) inputStream.close()
    }
  }

}
