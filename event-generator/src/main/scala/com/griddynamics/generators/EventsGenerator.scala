package com.griddynamics.generators

import org.scalacheck.Gen

case class IpAddress(firstDomain: Int, secondDomain: Int, thirdDomain: Int, fourthDomain: Int) {
  def literalName: String = s"$firstDomain.$secondDomain.$thirdDomain.$fourthDomain"
}

case class Event(eventType: String, ipAddress: String, eventTime: String, url: String)

object EventsGenerator {
  private val maxDelayConfig = "generator.max_delay_in_millis"
  private val redirectsFractionConfig = "generator.redirects_number"
  private val clicksFractionConfig = "generator.clicks_number"
  def apply(propertiesWrapper: PropertiesWrapper): EventsGenerator = {
    val maxDelayInMillis = propertiesWrapper.getLongProperty(maxDelayConfig, new java.lang.Long(1L))
    val redirectsFraction = propertiesWrapper.getIntProperty(redirectsFractionConfig, new Integer(1))
    val clicksFraction = propertiesWrapper.getIntProperty(clicksFractionConfig, new Integer(1))

    EventsGenerator(maxDelayInMillis, redirectsFraction, clicksFraction)
  }
}

case class EventsGenerator(maxDelayInMillis: Long = 100000L,
                           redirectsFraction: Int = 5,
                           clicksFraction: Int = 95) {

  def generatePortionOfEvents(numberToGenerate: Int): Gen[List[Event]] =
    Gen.listOfN(numberToGenerate, generateEvent())

  def generateEvent(): Gen[Event] = {
    for {
      eventType <- generateType()
      ipAddress <- generateIpAddress()
      eventTime <- generateEventTime()
      url <- generateUrl()
    } yield Event(eventType, ipAddress, eventTime, url)
  }


  def generateIpAddress(): Gen[String] = {
    val domainGen: Gen[Int] = Gen.chooseNum(0, 255)

    for {
      firstDomain <- domainGen
      secondDomain <- domainGen
      thirdDomain <- domainGen
      fourthDomain <- domainGen
    } yield IpAddress(firstDomain, secondDomain, thirdDomain, fourthDomain).literalName
  }

  def generateUrl(): Gen[String] = Gen
    .oneOf("https://www.google.com/url?q=https://blog.griddynamics.com/in-stream-processing-service-blueprint",
      "https://www.nurkiewicz.com/2014/09/property-based-testing-with-scalacheck.html",
      "https://yandex.ru",
      "https://www.google.com",
      "https://mvnrepository.com/artifact/org.scalacheck/scalacheck_2.11/1.14.0"
    )

  def generateEventTime(): Gen[String] = {
    Gen.chooseNum(0, maxDelayInMillis)
      .map(l => (System.currentTimeMillis() - l).toString)

  }

  def generateType(): Gen[String] = {
    Gen.frequency((redirectsFraction, Gen.const("redirect")), (clicksFraction, Gen.const("click")))
  }


}
