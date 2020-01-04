package com.griddynamics.generators

import play.api.libs.json._

object EventToJsonSerializer {
  def eventToJson(event: Event): JsValue = {
    implicit val eventWrites: Writes[Event] = new Writes[Event] {
      def writes(event: Event): JsValue = Json.obj(
        "eventType" -> event.eventType,
        "ipAddress" -> event.ipAddress,
        "eventTime" -> event.eventTime,
        "url" -> event.url
      )
    }
    Json.toJson(event)
  }
}
