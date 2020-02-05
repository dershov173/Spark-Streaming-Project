package com.griddynamics.kafka.connector

import org.apache.hadoop.fs.Path
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.{FlatSpec, Matchers}

class MultiTasksEventTimestampPathFilterTest extends FlatSpec with Matchers {

  trait MultiTasksEventTimestampPathFilterTestContext {
    val timestamp: Long = 0L
    val lastUploadedFileTimestamp: Long = 0L
    val internalId: Long = 0L
    val tasksNum: Int = 0
    val currentTaskId: Int = 0
    val pathName: String

    def filterParams: FilterParams = FilterParams(internalId,
      lastUploadedFileTimestamp,
      tasksNum,
      currentTaskId
    )
  }

  "MultiTasksEventTimestampPathFilter" should "return false for corrupted file names" in
    new MultiTasksEventTimestampPathFilterTestContext {
      override val pathName: String = arbitrary[String].suchThat(!_.isEmpty).sample.get
      override val tasksNum: Int = Gen.posNum[Int].sample.get
      override val currentTaskId: Int = Gen.choose(1, tasksNum).sample.get

      val path = new Path(pathName)

      assert(!MultiTasksEventTimestampPathFilter(filterParams).accept(path))
    }

  "MultiTasksEventTimestampPathFilter" should "return false for all files uploaded earlier than lastUploadedFileTimestamp" in
    new MultiTasksEventTimestampPathFilterTestContext {
      override val timestamp: Long = Gen.posNum[Long].sample.get
      override val lastUploadedFileTimestamp: Long = Gen.chooseNum(timestamp + 1, Long.MaxValue).sample.get
      override val internalId: Long = Gen.chooseNum(0L, Long.MaxValue).sample.get
      override val tasksNum: Int = Gen.choose(2, Int.MaxValue).sample.get
      override val currentTaskId: Int = Gen.choose(0, tasksNum).sample.get
      override val pathName: String = s"${timestamp}_$internalId.csv"

      val path = new Path(pathName)

      assert(!MultiTasksEventTimestampPathFilter(filterParams).accept(path))
    }


  "MultiTasksEventTimestampPathFilter" should "return true for a file if it has been  uploaded after lastUploadedFileTimestamp" +
    "and belongs given task" in
    new MultiTasksEventTimestampPathFilterTestContext {
      override val lastUploadedFileTimestamp: Long = Gen.posNum[Long].sample.get
      override val timestamp: Long = Gen.chooseNum(lastUploadedFileTimestamp + 1, Long.MaxValue).sample.get
      override val internalId: Long = Gen.chooseNum(0L, Long.MaxValue).sample.get
      override val tasksNum: Int = Gen.choose(2, Int.MaxValue).sample.get
      override val currentTaskId: Int = (internalId % tasksNum).intValue
      override val pathName: String = s"${timestamp}_$internalId.csv"

      val path = new Path(pathName)

      assert(MultiTasksEventTimestampPathFilter(filterParams).accept(path))
    }

  "MultiTasksEventTimestampPathFilter" should "return true for a file if it has been  uploaded after lastUploadedFileTimestamp" +
    "and belongs task 0" in
    new MultiTasksEventTimestampPathFilterTestContext {
      override val lastUploadedFileTimestamp: Long = Gen.posNum[Long].sample.get
      override val timestamp: Long = Gen.chooseNum(lastUploadedFileTimestamp + 1, Long.MaxValue).sample.get
      override val tasksNum: Int = Gen.choose(2, Int.MaxValue).sample.get
      override val internalId: Long = tasksNum
      override val currentTaskId: Int = 0
      override val pathName: String = s"${timestamp}_$internalId.csv"

      val path = new Path(pathName)

      assert(MultiTasksEventTimestampPathFilter(filterParams).accept(path))
    }


  "MultiTasksEventTimestampPathFilter" should "return true for a file if it has been  uploaded after lastUploadedFileTimestamp" +
    "and belongs task taskNum - 1" in
    new MultiTasksEventTimestampPathFilterTestContext {
      override val lastUploadedFileTimestamp: Long = Gen.posNum[Long].sample.get
      override val timestamp: Long = Gen.chooseNum(lastUploadedFileTimestamp + 1, Long.MaxValue).sample.get
      override val tasksNum: Int = Gen.choose(2, Int.MaxValue).sample.get
      override val internalId: Long = tasksNum - 1
      override val currentTaskId: Int = tasksNum - 1
      override val pathName: String = s"${timestamp}_$internalId.csv"

      val path = new Path(pathName)

      assert(MultiTasksEventTimestampPathFilter(filterParams).accept(path))
    }


}
