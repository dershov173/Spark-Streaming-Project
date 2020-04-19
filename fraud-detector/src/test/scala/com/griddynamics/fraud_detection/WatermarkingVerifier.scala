package com.griddynamics.fraud_detection

import java.io.{BufferedOutputStream, ObjectInputStream, ObjectOutputStream, PrintStream}
import java.net.{InetAddress, ServerSocket, Socket}
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.griddynamics.generators.{EventToJsonSerializer, IpAddress}
import org.apache.spark.sql.SparkSession
import org.scalacheck.Gen.Parameters
import org.scalacheck.rng.Seed
import org.scalatest.{FlatSpec, FunSuite, Matchers}


class WatermarkingVerifier extends FlatSpec with Matchers {
  private val mockTime: LocalDateTime = LocalDateTime.of(2020, 4, 17, 20, 0, 0)
  private val zonedMockTime: ZonedDateTime = ZonedDateTime
    .of(mockTime, ZoneId.systemDefault())
  private val t0: Long = zonedMockTime
    .toInstant
    .getEpochSecond


  "do something" should "do something" in {

    val ipAddress = IpAddress(192, 168, 0, 1)

    val t1 = ZonedDateTime.now()

    val processedEvents = SimpleEventDatasetGenerator(t1, 0, 9, 25, ipAddress)
    val eventsArray = processedEvents
      .generateTestEventArray()
      .pureApply(Parameters.default.withSize(1), Seed.random(), 1)
      .map(EventToJsonSerializer.serialize(_).get)

    val server = new ServerSocket(9999)
    val s = server.accept()
    val out = new PrintStream(
      new BufferedOutputStream(s.getOutputStream))
    while (true) {

      eventsArray
        .foreach(out.println)
      out.flush()

      Thread.sleep(2000)
    }
    s.close()
  }
}
