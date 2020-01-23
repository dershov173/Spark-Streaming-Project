package com.griddynamics.generators

import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json.{JsSuccess, Json, Reads}

class EventToJsonSerializerTest extends FlatSpec with Matchers with MockFactory {
  private def generateEvent(): Gen[Event] = {
    for {
      eventType <- Gen.alphaStr
      ipAddress <- Gen.alphaStr
      eventTime <- Gen.alphaStr
      url <- Gen.alphaStr
    } yield Event(eventType, ipAddress, eventTime, url)
  }

  "events" should "be properly serialized to Json" in {
    implicit val eventFormat:Reads[Event] = Json.format[Event]
    val events = Gen.listOf(generateEvent()).sample.get
    events
      .foreach(e => EventToJsonSerializer
        .serialize(e)
        .get
        .validate match {
        case JsSuccess(value, path) => assert(value === e)
        case _ => assert(false)
      })
  }
}
