package com.griddynamics.generators

import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsValue

import scala.util.Try


object EventsWriter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val numberToGenerateConfig = "generator.events_number"

  def apply(propertiesWrapper: PropertiesWrapper): EventsWriter = new EventsWriter(
    FSOperationsMaintainer(propertiesWrapper),
    EventsGenerator(propertiesWrapper),
    propertiesWrapper.getIntProperty(numberToGenerateConfig, new Integer(1)))
}

case class EventsWriter(fsOperationsMaintainer: FSOperationsMaintainer,
                        eventsGenerator: EventsGenerator,
                        numberOfEventsToGenerate: Int = 1) extends Cloneable {

  import com.griddynamics.generators.EventsWriter._
  def writeEvents(): Unit = {
      def writeEventToGeneratedHDFSPath(tryJson: Try[JsValue]): Try[Unit] = {
        tryJson.flatMap(json =>
          fsOperationsMaintainer
            .writeToHDFS(fsOperationsMaintainer.generateUniquePath, json.toString()))
      }

      getEvents
        .map(EventToJsonSerializer.serialize)
        .map(writeEventToGeneratedHDFSPath)
        .find(_.isFailure) match {
        case None =>
          logger.info("Process has finished successfully")
        case Some(exception) =>
          logger.error("There was an exception occurred", exception.failed.get)
      }
  }

  def close(): Unit = fsOperationsMaintainer.close()

  private def getEvents: List[Event] = {
    eventsGenerator
      .generatePortionOfEvents(numberOfEventsToGenerate)
      .sample
      .get
  }


}
