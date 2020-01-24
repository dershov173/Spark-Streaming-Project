package com.griddynamics.kafka.connector

import org.apache.hadoop.fs.Path
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

class EventIdFromFSPathConstructorTest extends FlatSpec with Matchers with MockFactory {
  "mapper" should "properly parse path \\d+_\\d+ file name" in {
    val timestamp = Gen.posNum[Long].sample.get
    val internalId = Gen.chooseNum(0L, Long.MaxValue).sample.get
    val pathName = s"${timestamp}_$internalId"

    val expectedIdentifier = EventIdentifier(timestamp, internalId, null, pathName)

    val path = new Path(pathName)

    assert(Success(expectedIdentifier) === EventIdFromFSPathConstructor().constructId(path))
  }

  "mapper" should "properly parse path \\d+_\\d+_\\s file name" in {
    val timestamp = Gen.posNum[Long].sample.get
    val internalId = Gen.chooseNum(0L, Long.MaxValue).sample.get
    val instanceName = Gen.alphaStr.sample.get
    val pathName = s"${timestamp}_${internalId}_$instanceName"

    val expectedIdentifier = EventIdentifier(timestamp, internalId, instanceName, pathName)

    val path = new Path(pathName)

    assert(Success(expectedIdentifier) === EventIdFromFSPathConstructor().constructId(path))
  }

  "mapper" should "return failure in case if there are more than 3 groups" in {
    val timestamp = Gen.posNum[Long].sample.get
    val internalId = Gen.chooseNum(0L, Long.MaxValue).sample.get
    val instanceName1 = Gen.alphaStr.sample.get
    val instanceName2 = Gen.alphaStr.sample.get
    val pathName = s"${timestamp}_${internalId}_${instanceName1}_${instanceName2}"

    val path = new Path(pathName)

    assert(EventIdFromFSPathConstructor().constructId(path).isFailure)
  }

  "mapper" should "return failure in case if path totally violates pattern" in {
    val pathName = Gen.alphaStr.sample.get

    val path = new Path(pathName)

    assert(EventIdFromFSPathConstructor().constructId(path).isFailure)
  }


}
