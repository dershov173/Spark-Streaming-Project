package com.griddynamics.generators

import play.api.libs.json._

import scala.util.Try

trait EventSerializer {
  def serialize(event: Event) : Try[JsValue]
}

object EventToJsonSerializer extends EventSerializer {
  override def serialize(event: Event): Try[JsValue] = {
    implicit val eventWrites: Writes[Event] = new Writes[Event] {
      def writes(event: Event): JsValue = Json.obj(
        "eventType" -> event.eventType,
        "ipAddress" -> event.ipAddress,
        "eventTime" -> event.eventTime,
        "url" -> event.url
      )
    }
    Try(Json.toJson(event))
  }
}
