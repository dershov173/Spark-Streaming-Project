package com.griddynamics.kafka.connector

import org.apache.hadoop.fs.Path
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

class EventTimestampPathFilterTest extends FlatSpec with Matchers with MockFactory {
  "filter" should "return false for corrupted file names" in {
    val pathName = Gen.alphaStr.sample.get

    val path = new Path(pathName)

    assert(!EventTimestampPathFilter(0L).accept(path))
  }

  "filter" should "return false for all files uploaded earlier than lastUploadedFileTimestamp" in {
    val timestamp = Gen.posNum[Long].sample.get
    val lastUploadedFileTimestamp = Gen.chooseNum(timestamp, Long.MaxValue).sample.get
    val internalId = Gen.chooseNum(0L, Long.MaxValue).sample.get

    val path = new Path(s"${timestamp}_$internalId.csv")

    assert(!EventTimestampPathFilter(lastUploadedFileTimestamp).accept(path))
  }

  "filter" should "return true for all files uploaded after lastUploadedFileTimestamp" in {
    val lastUploadedFileTimestamp = Gen.posNum[Long].sample.get
    val timestamp = Gen.chooseNum(lastUploadedFileTimestamp + 1, Long.MaxValue).sample.get
    val internalId = Gen.chooseNum(0L, Long.MaxValue).sample.get

    val path = new Path(s"${timestamp}_$internalId.json")

    assert(EventTimestampPathFilter(lastUploadedFileTimestamp).accept(path))
  }

}
