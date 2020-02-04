package com.griddynamics.kafka.connector

import org.apache.hadoop.fs.{Path, PathFilter}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}


object PathFilters {
  def getPathFilter(filterParams: FilterParams): PathFilter = {
    if (filterParams.tasksNumber == 1) new SingleTaskEventTimestampPathFilter(filterParams)
    else MultiTasksEventTimestampPathFilter(filterParams)
  }
}

case class FilterParams(lastUploadedFileInternalId: Long,
                        generatedTimestamp: Long,
                        tasksNumber: Int,
                        taskId: Int)


case class SingleTaskEventTimestampPathFilter(lastUploadedFileInternalId: Long,
                                              generatedTimestamp: Long) extends PathFilter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def this(filterParams: FilterParams) = this(lastUploadedFileInternalId = filterParams.lastUploadedFileInternalId,
    generatedTimestamp = filterParams.generatedTimestamp)

  override def accept(path: Path): Boolean = {
    logger.info("SingleTaskEventTimestampPathFilter with following lastUploadedFileTimestamp= {} and internalId = {} has been applied",
      generatedTimestamp,
      lastUploadedFileInternalId)
    EventIdFromFSPathConstructor()
      .constructId(path) match {
      case Success(eventIdentifier) =>
        eventIdentifier.timestamp > generatedTimestamp ||
          (eventIdentifier.timestamp == generatedTimestamp &&
            eventIdentifier.internalId > lastUploadedFileInternalId)

      case Failure(exception) =>
        logger.error(s"There was an exception occurred while trying to process file ${path}", exception)
        false
    }
  }
}

case class MultiTasksEventTimestampPathFilter(filterParams: FilterParams) extends PathFilter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val generatedTimestamp = filterParams.generatedTimestamp
  private val lastUploadedFileInternalId = filterParams.lastUploadedFileInternalId
  private val tasksNumber = filterParams.tasksNumber
  private val currentTaskId = filterParams.taskId

  override def accept(path: Path): Boolean = {
    logger.debug("MultiTasksEventTimestampPathFilter with following lastUploadedFileTimestamp= {} and" +
      "internalId = {} has been applied. Tasks number = {}, current task = {}",
      generatedTimestamp.toString,
      lastUploadedFileInternalId.toString,
      tasksNumber.toString,
      currentTaskId.toString)
    EventIdFromFSPathConstructor()
      .constructId(path) match {
      case Success(eventIdentifier) =>
        (eventIdentifier.timestamp > generatedTimestamp ||
          (eventIdentifier.timestamp == generatedTimestamp &&
            eventIdentifier.internalId > lastUploadedFileInternalId)) &&
          eventIdentifier.internalId % tasksNumber == currentTaskId

      case Failure(exception) =>
        logger.error(s"There was an exception occurred while trying to process file ${path}", exception)
        false
    }
  }
}
