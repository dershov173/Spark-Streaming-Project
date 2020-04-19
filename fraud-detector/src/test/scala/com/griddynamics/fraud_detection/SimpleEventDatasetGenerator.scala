package com.griddynamics.fraud_detection

import java.time.ZonedDateTime

import com.griddynamics.generators.{Event, IpAddress}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalacheck.Gen

import scala.collection.mutable.ArrayBuffer

case class SimpleEventDatasetGenerator(t0:ZonedDateTime,
                                       windowStart: Int,
                                       windowDuration: Int,
                                       numberOfRequestsPerSlide: Int,
                                       singleIp: IpAddress) {
  def generateTestEventDataset()(implicit sparkSession: SparkSession): Gen[Dataset[Event]] = {
    val eventGen = generateTestData()
    import sparkSession.sqlContext.implicits._
    Gen.listOfN(numberOfRequestsPerSlide, eventGen)
      .map(_.toDS())
  }

  def generateTestEventArray() : Gen[ArrayBuffer[Event]] = {
    val eventGen = generateTestData()
    Gen.buildableOfN[ArrayBuffer[Event], Event](numberOfRequestsPerSlide,eventGen)
  }

  private def generateTestData(): Gen[Event] = {
    val ipAddressGen = Gen.const(singleIp)
    val eventTimeGen = Gen
      .chooseNum(windowStart, windowStart + windowDuration)
      .map(t0.plusSeconds(_).toInstant.getEpochSecond)

    for {
      eventType <- Gen.alphaStr
      ipAddress <- ipAddressGen
      eventTime <- eventTimeGen
      url <- Gen.alphaStr
    } yield Event(eventType, ipAddress.literalName, eventTime.toString, url)
  }
}
