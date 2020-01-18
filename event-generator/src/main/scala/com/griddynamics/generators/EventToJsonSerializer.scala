package com.griddynamics.generators

import play.api.libs.json._

import scala.util.{Success, Try}

object EventToJsonSerializer {
  def eventToJson(event: Event): Try[JsValue] = {
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

object EventFromJsonDeserializer {
  def eventFromJson(json:String) : Try[Event] = {
    implicit val eventFormat:Reads[Event] = Json.format[Event]
    JsResult.toTry(
      Json.fromJson(Json.parse(json)),
      err => new RuntimeException(s"Cannot parse json $json. Following error" +
        s"occurred $err"))
  }
}
