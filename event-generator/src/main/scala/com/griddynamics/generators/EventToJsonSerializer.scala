package com.griddynamics.generators

import play.api.libs.json._

import scala.util.Try

trait EventSerializer {
  def serialize(event: Event) : Try[String]
}

object EventToJsonSerializer extends EventSerializer {
  override def serialize(event: Event): Try[String] = {
    implicit val eventWrites: Writes[Event] = (event: Event) => Json.obj(
      "eventType" -> event.eventType,
      "ipAddress" -> event.ipAddress,
      "eventTime" -> event.eventTime,
      "url" -> event.url
    )
    Try(Json.toJson(event).toString())
  }
}
