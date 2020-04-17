package com.griddynamics.fraud_detection

import java.sql.Timestamp

import com.griddynamics.generators.Event
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Dataset, SparkSession}


case class BotsIdentifier(windowDuration: String = "1 minute",
                          slideDuration: String = "10 seconds",
                          requestsPerGivenTimePeriodToBeRecognisedAsABot: Int = 20)
                         (implicit val sparkSession: SparkSession) {
  def identifyBots(events: Dataset[Event]): Dataset[DetectedBot] = {
    import sparkSession.implicits._

    val fromUnixTimestamp = from_unixtime($"eventTime")

    events
      .withColumn("eventTimestamp", fromUnixTimestamp)
      .groupBy(
        window(col("eventTimestamp"),
          "10 seconds",
          "10 seconds"),
        col("ipAddress")
      )
      .count()
      .where($"count" > requestsPerGivenTimePeriodToBeRecognisedAsABot)
      .withColumn("detectionTime", unix_timestamp($"window.end"))
      .groupBy($"ipAddress")
      .agg(max($"detectionTime").as("detectionTime"))
      .as[DetectedBot]
  }
}

case class DetectedBot(detectionTime:String, ipAddress: String)

