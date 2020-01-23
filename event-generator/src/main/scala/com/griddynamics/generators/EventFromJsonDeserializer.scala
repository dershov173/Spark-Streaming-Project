package com.griddynamics.generators

import play.api.libs.json.{JsResult, Json, Reads}

import scala.util.Try

trait EventDeserializer {
  def deserialize(json:String): Try[Event]
}

object EventFromJsonDeserializer extends EventDeserializer {
  override def deserialize(json:String) : Try[Event] = {
    implicit val eventFormat:Reads[Event] = Json.format[Event]
    JsResult.toTry(
      Json.fromJson(Json.parse(json)),
      err => new RuntimeException(s"Cannot parse json $json. Following error" +
        s"occurred $err"))
  }
}
