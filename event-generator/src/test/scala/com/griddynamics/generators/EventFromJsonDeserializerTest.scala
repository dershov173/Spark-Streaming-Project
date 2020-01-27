package com.griddynamics.generators

import org.scalatest.{FlatSpec, Matchers}

class EventFromJsonDeserializerTest extends FlatSpec with Matchers {
  "deserializer" should "deserialize events" in {
    val serializer: EventToJsonSerializer.type = EventToJsonSerializer
    val deserializer = EventFromJsonDeserializer
    val events = EventsGenerator()
      .generatePortionOfEvents(2)
      .sample
      .get
    val serializedEvents = events
      .map(serializer.serialize)
        .map(_.get)
    serializedEvents.foreach(println)
    val triedEvents = serializedEvents
      .map(deserializer.deserialize)
      .map(_.get)
    triedEvents.foreach(println)
    assert(events == triedEvents)
  }

  "deserializer" should "deserialize some event" in {
    val eventString = "?{\"eventType\":\"click\",\"ipAddress\":\"83.0.232.0\",\"eventTime\":\"1580126003372\",\"url\":\"https://mvnrepository.com/artifact/org.scalacheck/scalacheck_2.11/1.14.0\"}"
    println(EventFromJsonDeserializer.deserialize(eventString))
  }
}
