package com.griddynamics.fraud_detection

import com.griddynamics.generators.{Event, EventsGenerator, IpAddress}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}

sealed trait GeneratedDatasetDresser[T] {
  def enrichGenDataset() : Gen[Dataset[T]]
}

case class SimpleGeneratedEventDatasetDresser(t0:Long,
                                              slideStart: Int,
                                              slideDuration: Int,
                                              numberOfRequestsPerSlide: Int,
                                              singleIp: IpAddress)
                                             (implicit val sparkSession: SparkSession) extends GeneratedDatasetDresser[Event] {
  override def enrichGenDataset(): Gen[Dataset[Event]] = {
    val ipAddressGen = Gen.const(singleIp)
    val eventTimeGen = Gen
      .chooseNum(slideStart, slideStart + slideDuration)
      .map(t0 + _)

    val eventGen = for {
      eventType <- Gen.alphaStr
      ipAddress <- ipAddressGen
      eventTime <- eventTimeGen
      url <- Gen.alphaStr
    } yield Event(eventType, ipAddress.literalName, eventTime.toString, url)

    import sparkSession.sqlContext.implicits._
    val eventsNumber = numberOfRequestsPerSlide
    Gen.listOfN(eventsNumber, eventGen)
      .map(_.toDS())
  }
}

case class ComposedGeneratedEventDatasetDresser(generatedDatasetDresserLeft: GeneratedDatasetDresser[Event],
                                                generatedDatasetDresserRight: GeneratedDatasetDresser[Event])
                                             (implicit val sparkSession: SparkSession) extends GeneratedDatasetDresser[Event] {
  override def enrichGenDataset(): Gen[Dataset[Event]] = {
    for {
      dataset1 <- generatedDatasetDresserLeft.enrichGenDataset()
      dataset2 <- generatedDatasetDresserRight.enrichGenDataset()
    } yield dataset1.union(dataset2)
  }

}
