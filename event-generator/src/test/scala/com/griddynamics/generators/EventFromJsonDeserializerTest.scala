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
}
