package com.griddynamics.generators

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsValue

import scala.util.{Failure, Success, Try}

case class EventsWriter(propertiesWrapper: PropertiesWrapper) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val defaultFSConfig = "fs.defaultFS"
  private val useDatanodeHostnameConfig = "dfs.client.use.datanode.hostname"

  private val numberToGenerateConfig = "generator.events_number"
  private val maxDelayConfig = "generator.max_delay_in_millis"
  private val redirectsFractionConfig = "generator.redirects_number"
  private val clicksFractionConfig = "generator.clicks_number"
  private val fsEventsDirectoryConfig = "fs.events.directory"

  def writeEvents(): Unit = {
    val maintainer = getFSOperationsMaintainer

    try {
      def writeEventToGeneratedHDFSPath(tryJson: Try[JsValue]): Try[Unit] = {
        tryJson.flatMap(json =>
          maintainer
            .writeToHDFS(maintainer.generateUniquePath, json.toString()))
      }

      getEvents
        .map(EventToJsonSerializer.eventToJson)
        .map(writeEventToGeneratedHDFSPath)
        .find(_.isFailure) match {
        case None =>
          logger.info("Process has finished successfully")
        case Some(exception) =>
          logger.error("There was an exception occurred", exception.failed.get)
      }
    } catch {
      case t: Throwable => logger.error("There was an exception occurred", t)
    } finally {
      if (maintainer != null) maintainer.close()
    }
  }

  private def getEvents: List[Event] = {
    val eventsToGenerate = propertiesWrapper.getIntProperty(numberToGenerateConfig, new Integer(1))
    val maxDelayInMillis = propertiesWrapper.getLongProperty(maxDelayConfig, new java.lang.Long(1L))
    val redirectsFraction = propertiesWrapper.getIntProperty(redirectsFractionConfig, new Integer(1))
    val clicksFraction = propertiesWrapper.getIntProperty(clicksFractionConfig, new Integer(1))

    val eventsGenerator = EventsGenerator(maxDelayInMillis, redirectsFraction, clicksFraction)

    eventsGenerator
      .generatePortionOfEvents(eventsToGenerate)
      .sample
      .get
  }

  private def getFSOperationsMaintainer: FSOperationsMaintainer = {
    val defaultFS = propertiesWrapper.getProperty(defaultFSConfig)
    if (defaultFS == null) throw
      new IllegalArgumentException("There is no required config fs.defaultFS set ")
    val useDatanodeHostname = propertiesWrapper
      .getBooleanProperty(useDatanodeHostnameConfig, true)
    val eventsDirectoryName = propertiesWrapper
      .getOrDefaultString(fsEventsDirectoryConfig, "/events")

    val configuration = new Configuration()
    configuration.set(defaultFSConfig, defaultFS)
    configuration.set(useDatanodeHostnameConfig, useDatanodeHostname.toString)

    FSOperationsMaintainer(configuration, eventsDirectoryName)
  }
}
