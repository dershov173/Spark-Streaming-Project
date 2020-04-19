package com.griddynamics.fraud_detection

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import com.griddynamics.fraud_detection.decorators.GenDatasetGeneratorDecorator
import com.griddynamics.generators.{Event, IpAddress}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalacheck.Gen
import org.scalacheck.Gen.Parameters
import org.scalacheck.rng.Seed
import org.scalatest.{FlatSpec, Matchers}

class BotsIdentifierTest extends FlatSpec with Matchers {
  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("BotsIdentifierTest")
    .getOrCreate()

  private val mockTime: LocalDateTime = LocalDateTime.of(2020, 4, 17, 20, 0, 0)
  private val zonedMockTime: ZonedDateTime = ZonedDateTime
    .of(mockTime, ZoneId.systemDefault())
  "identifier" should "recognise 1 bot" in {
    val ipAddress = IpAddress(192, 168, 0, 1)

    val datasetGenerator = SimpleEventDatasetGenerator(zonedMockTime, 0, 9, 25, ipAddress)
      .generateTestEventDataset()

    val processedEvents = processTestDatasets(datasetGenerator)

    processedEvents.length shouldEqual 1
    processedEvents(0).ipAddress shouldEqual ipAddress.literalName
  }

  "identifier" should "recognise 1 bot and 1 good guy" in {
    val botIp = IpAddress(192, 168, 0, 1)
    val goodGuyIp = IpAddress(174, 10, 11, 91)
    val normalEventsNumber = 10
    val suspiciousEventsNumber = 30
    val suspiciousEventsGenerator = SimpleEventDatasetGenerator(zonedMockTime, 0, 9, suspiciousEventsNumber, botIp)
      .generateTestEventDataset()
    val normalEventsGenerator = SimpleEventDatasetGenerator(zonedMockTime, 0, 9, normalEventsNumber, goodGuyIp)
      .generateTestEventDataset()

    val processedEvents = processTestDatasets(suspiciousEventsGenerator,
      normalEventsGenerator)

    processedEvents.length should be (1)
    processedEvents.head.ipAddress should be (botIp.literalName)
  }

  "identifier" should "not find any bot in both sliding windows" in {
    val goodGuyIp = IpAddress(174, 10, 11, 91)
    val eventsNumberPerFirstSlide = 17
    val eventsNumberPerSecondSlide = 19
    val firstSlideEventDataGenerator = SimpleEventDatasetGenerator(zonedMockTime, 0, 9, eventsNumberPerFirstSlide, goodGuyIp)
      .generateTestEventDataset()
    val secondSlideEventDataGenerator = SimpleEventDatasetGenerator(zonedMockTime, 10, 9, eventsNumberPerSecondSlide, goodGuyIp)
      .generateTestEventDataset()

    val processedEvents = processTestDatasets(firstSlideEventDataGenerator,
      secondSlideEventDataGenerator)

    processedEvents.length should be (0)
  }

  "identifier" should "identify 1 bot in the second sliding window" in {
    val botIp = IpAddress(174, 10, 11, 91)
    val eventsNumberPerFirstSlide = 17
    val eventsNumberPerSecondSlide = 23
    val firstSlideEventDataGenerator = SimpleEventDatasetGenerator(zonedMockTime, 0, 9, eventsNumberPerFirstSlide, botIp)
      .generateTestEventDataset()
    val secondSlideEventDataGenerator = SimpleEventDatasetGenerator(zonedMockTime, 10, 9, eventsNumberPerSecondSlide, botIp)
      .generateTestEventDataset()

    val processedEvents = processTestDatasets(firstSlideEventDataGenerator,
      secondSlideEventDataGenerator)

    processedEvents.length should be (1)
    processedEvents.head.ipAddress should be (botIp.literalName)
    val unixTimestamp = processedEvents.head.detectionTime.toLong
    val lastDetectionTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), ZoneId.systemDefault())
    lastDetectionTime should be (zonedMockTime.plusSeconds(10))
  }

  "identifier" should "recognise 1 bot in the first sliding window" +
    "for a certain ip address and" +
    "1 bot in the second sliding window for a different ip address" in {
    val bot1 = IpAddress(192, 168, 0, 1)
    val bot2 = IpAddress(174, 10, 11, 91)
    val bot1RequestsPerFirstSlide = 100
    val bot1RequestsPerSecondSlide = 10
    val bot2requestsPerFirstSlide = 3
    val bot2requestsPerSecondSlide = 30

    val bot1FirstSlideEventDataGenerator = SimpleEventDatasetGenerator(zonedMockTime, 0, 9, bot1RequestsPerFirstSlide, bot1)
      .generateTestEventDataset()
    val bot1SecondSlideEventDataGenerator = SimpleEventDatasetGenerator(zonedMockTime, 10, 9, bot1RequestsPerSecondSlide, bot1)
      .generateTestEventDataset()

    val bot2FirstSlideEventDataGenerator = SimpleEventDatasetGenerator(zonedMockTime, 0, 9, bot2requestsPerFirstSlide, bot2)
      .generateTestEventDataset()
    val bot2SecondSlideEventDataGenerator = SimpleEventDatasetGenerator(zonedMockTime, 10, 9, bot2requestsPerSecondSlide, bot2)
      .generateTestEventDataset()

    val processedEvents = processTestDatasets(bot1FirstSlideEventDataGenerator,
      bot1SecondSlideEventDataGenerator,
      bot2FirstSlideEventDataGenerator,
      bot2SecondSlideEventDataGenerator)


    processedEvents.length should be (2)
    assertDetectionTime(processedEvents.find(_.ipAddress == bot1.literalName).get,
      zonedMockTime)
    assertDetectionTime(processedEvents.find(_.ipAddress == bot2.literalName).get,
      zonedMockTime.plusSeconds(10))

  }

  private def processTestDatasets(gen0 :Gen[Dataset[Event]],
                                  gens :Gen[Dataset[Event]]*) : Array[DetectedBot] = {
    val composedDatasetGenerator = new GenDatasetGeneratorDecorator()
      .enrichGenData(gen0, gens : _*)

    composedDatasetGenerator
      .map(BotsIdentifier().identifyBots)
      .map(_.collect())
      .pureApply(Parameters.default, Seed.random(),1)
  }

  private def assertDetectionTime(detectedBot: DetectedBot, expectedDetectionTime: ZonedDateTime) : Unit = {
    val unixTimestamp = detectedBot.detectionTime.toLong
    val lastDetectionTime = ZonedDateTime
      .ofInstant(Instant.ofEpochSecond(unixTimestamp),
        ZoneId.systemDefault())
    lastDetectionTime should be (expectedDetectionTime)
  }

}
