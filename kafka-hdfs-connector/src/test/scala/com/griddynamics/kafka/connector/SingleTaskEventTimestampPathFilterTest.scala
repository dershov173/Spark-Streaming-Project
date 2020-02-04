package com.griddynamics.kafka.connector

import org.apache.hadoop.fs.Path
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.Arbitrary.arbitrary

class SingleTaskEventTimestampPathFilterTest extends FlatSpec with Matchers with MockFactory {
  "filter" should "return false for corrupted file names" in {
    val pathName = arbitrary[String].suchThat(!_.isEmpty).sample.get

    val path = new Path(pathName)

    assert(!SingleTaskEventTimestampPathFilter(0L, 0L).accept(path))
  }

  "filter" should "return false for all files uploaded earlier than lastUploadedFileTimestamp" in {
    val timestamp = Gen.posNum[Long].sample.get
    val lastUploadedFileTimestamp = Gen.chooseNum(timestamp + 1, Long.MaxValue).sample.get
    val internalId = Gen.chooseNum(0L, Long.MaxValue).sample.get

    val path = new Path(s"${timestamp}_$internalId.csv")

    assert(!SingleTaskEventTimestampPathFilter(0L, lastUploadedFileTimestamp).accept(path))
  }

  "filter" should "return true for all files uploaded after lastUploadedFileTimestamp" in {
    val lastUploadedFileTimestamp = Gen.posNum[Long].sample.get
    val timestamp = Gen.chooseNum(lastUploadedFileTimestamp + 1, Long.MaxValue).sample.get
    val internalId = Gen.posNum[Long].sample.get

    val path = new Path(s"${timestamp}_$internalId.json")

    assert(SingleTaskEventTimestampPathFilter(0L, lastUploadedFileTimestamp).accept(path))
  }

  "filter" should "return true for all files uploaded at lastUploadedFileTimestamp with higher internalId" in {
    val lastUploadedFileTimestamp = Gen.posNum[Long].sample.get
    val prevInternalId = Gen.posNum[Long].sample.get
    val internalId = Gen.choose(prevInternalId, Long.MaxValue).sample.get

    val path = new Path(s"${lastUploadedFileTimestamp}_$internalId.json")

    assert(SingleTaskEventTimestampPathFilter(prevInternalId, lastUploadedFileTimestamp).accept(path))
  }

}
