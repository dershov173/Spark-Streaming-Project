package com.griddynamics.fraud_detection

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import com.griddynamics.generators.IpAddress
import org.apache.spark.sql.SparkSession
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
  private val t0: Long = zonedMockTime
    .toInstant
    .getEpochSecond
  "identifier" should "recognise 1 bot" in {
    val ipAddress = IpAddress(192, 168, 0, 1)

    val processedEvents = SimpleGeneratedEventDatasetDresser(t0, 0, 9, 25, ipAddress)
      .enrichGenDataset()
      .map(BotsIdentifier().identifyBots)
      .map(_.collect())
      .sample
      .get

    processedEvents.length should be (1)
  }

  "identifier" should "recognise 1 bot and 1 good guy" in {
    val botIp = IpAddress(192, 168, 0, 1)
    val goodGuyIp = IpAddress(174, 10, 11, 91)
    val normalEventsNumber = 10
    val suspiciousEventsNumber = 30
    val suspiciouesDresser = SimpleGeneratedEventDatasetDresser(t0, 0, 9, suspiciousEventsNumber, botIp)
    val goodDresser = SimpleGeneratedEventDatasetDresser(t0, 0, 9, normalEventsNumber, goodGuyIp)

    val composedGeneratedEventDatasetDresser = ComposedGeneratedEventDatasetDresser(suspiciouesDresser, goodDresser)


    val processedEvents = composedGeneratedEventDatasetDresser
        .enrichGenDataset()
      .map(BotsIdentifier().identifyBots)
      .map(_.collect())
      .sample
      .get


    processedEvents.length should be (1)
    processedEvents.head.ipAddress should be (botIp.literalName)
  }

  "identifier" should "not find any bot using 2 sliding windows" in {
    val goodGuyIp = IpAddress(174, 10, 11, 91)
    val eventsNumberPerFirstSlide = 17
    val eventsNumberPerSecondSlide = 19
    val firstSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 0, 9, eventsNumberPerFirstSlide, goodGuyIp)
    val secondSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 10, 9, eventsNumberPerSecondSlide, goodGuyIp)

    val composedGeneratedEventDatasetDresser = ComposedGeneratedEventDatasetDresser(firstSlideDresser, secondSlideDresser)


    val processedEvents = composedGeneratedEventDatasetDresser
        .enrichGenDataset()
      .map(BotsIdentifier().identifyBots)
      .map(_.collect())
      .sample
      .get


    processedEvents.length should be (0)

  }

  "identifier" should "identify 1 bot in second slide" in {
    val botIp = IpAddress(174, 10, 11, 91)
    val eventsNumberPerFirstSlide = 17
    val eventsNumberPerSecondSlide = 23
    val firstSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 0, 9, eventsNumberPerFirstSlide, botIp)
    val secondSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 10, 9, eventsNumberPerSecondSlide, botIp)

    val composedGeneratedEventDatasetDresser = ComposedGeneratedEventDatasetDresser(firstSlideDresser, secondSlideDresser)


    val processedEvents = composedGeneratedEventDatasetDresser
        .enrichGenDataset()
      .map(BotsIdentifier().identifyBots)
      .map(_.collect())
      .sample
      .get


    processedEvents.length should be (1)
    processedEvents.head.ipAddress should be (botIp.literalName)
    val unixTimestamp = processedEvents.head.detectionTime.toLong
    val lastDetectionTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), ZoneId.systemDefault())
    lastDetectionTime should be (zonedMockTime.plusSeconds(20))
  }

  "identifier" should "identify 1 bot in the second slide" +
    " although there is also one in the first slide " in {
    val botIp = IpAddress(174, 10, 11, 91)
    val eventsNumberPerFirstSlide = 22
    val eventsNumberPerSecondSlide = 23
    val firstSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 0, 9, eventsNumberPerFirstSlide, botIp)
    val secondSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 10, 9, eventsNumberPerSecondSlide, botIp)

    val composedGeneratedEventDatasetDresser = ComposedGeneratedEventDatasetDresser(firstSlideDresser, secondSlideDresser)


    val processedEvents = composedGeneratedEventDatasetDresser
        .enrichGenDataset()
      .map(BotsIdentifier().identifyBots)
      .map(_.collect())
      .sample
      .get


    processedEvents.length should be (1)
    processedEvents.head.ipAddress should be (botIp.literalName)
    assertDetectionTime(processedEvents.head,
      zonedMockTime.plusSeconds(20))
  }

  "identifier" should "recognise 1 bot in the first slide and" +
    "1 bot in the second" in {
    val bot1 = IpAddress(192, 168, 0, 1)
    val bot2 = IpAddress(174, 10, 11, 91)
    val bot1RequestsPerFirstSlide = 100
    val bot1RequestsPerSecondSlide = 10
    val bot2requestsPerFirstSlide = 3
    val bot2requestsPerSecondSlide = 30

    val bot1FirstSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 0, 9, bot1RequestsPerFirstSlide, bot1)
    val bot1SecondSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 10, 9, bot1RequestsPerSecondSlide, bot1)

    val bot1ComposedDresser = ComposedGeneratedEventDatasetDresser(bot1FirstSlideDresser, bot1SecondSlideDresser)

    val bot2FirstSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 0, 9, bot2requestsPerFirstSlide, bot2)
    val bot2SecondSlideDresser = SimpleGeneratedEventDatasetDresser(t0, 10, 9, bot2requestsPerSecondSlide, bot2)

    val bot2ComposedDresser = ComposedGeneratedEventDatasetDresser(bot2FirstSlideDresser, bot2SecondSlideDresser)

    val superDresser = ComposedGeneratedEventDatasetDresser(bot1ComposedDresser, bot2ComposedDresser)

    val processedEvents = superDresser
      .enrichGenDataset()
      .map(BotsIdentifier().identifyBots)
      .map(_.collect())
      .sample
      .get


    processedEvents.length should be (2)
    assertDetectionTime(processedEvents.find(_.ipAddress == bot1.literalName).get,
      zonedMockTime.plusSeconds(10))
    assertDetectionTime(processedEvents.find(_.ipAddress == bot2.literalName).get,
      zonedMockTime.plusSeconds(20))

  }

  private def assertDetectionTime(detectedBot: DetectedBot, expectedDetectionTime: ZonedDateTime) : Unit = {
    val unixTimestamp = detectedBot.detectionTime.toLong
    val lastDetectionTime = ZonedDateTime
      .ofInstant(Instant.ofEpochSecond(unixTimestamp),
        ZoneId.systemDefault())
    lastDetectionTime should be (expectedDetectionTime)
  }

}
