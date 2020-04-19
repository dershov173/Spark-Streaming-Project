package com.griddynamics.fraud_detection

import com.griddynamics.generators.Event
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}


case class BotsIdentifier(windowDuration: String = "10 seconds",
                          requestsPerGivenTimePeriodToBeRecognisedAsABot: Int = 20)
                         (implicit val sparkSession: SparkSession) {
  def identifyBots(events: Dataset[Event]): Dataset[DetectedBot] = {
    import sparkSession.implicits._

    val fromUnixTimestamp = from_unixtime($"eventTime")
            .cast("timestamp")

    events
      .withColumn("eventTimestamp", fromUnixTimestamp)
      .groupBy(
        $"ipAddress",
        window(col("eventTimestamp"),
          windowDuration))
      .agg(count("*").as("count"),
        min(unix_timestamp($"window.start")).as("detectionTime"))
      .where($"count" > requestsPerGivenTimePeriodToBeRecognisedAsABot)
      .select($"detectionTime", $"ipAddress")
      .as[DetectedBot]
  }
}

case class DetectedBot(detectionTime: String, ipAddress: String)

