package com.griddynamics.fraud_detection.decorators

import org.scalacheck.Gen
import org.scalacheck.Prop.Result
import org.scalatest.{FlatSpec, FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

class GenTraversableLikeDataGeneratorDecoratorTest extends FlatSpec with Matchers{
  "decorator" should "return the same generator " +
    "if only one has been provided to the method" in {
    val gen0 = Gen.listOf(Gen.posNum[Int])

    val decorator = new GenTraversableLikeDataGeneratorDecorator[Int]()

    decorator.enrichGenData(gen0) shouldEqual gen0
  }

  "decorator" should "properly combine two generators" in {
    val gen0 = Gen.listOf(Gen.posNum[Int])
    val gen1 = Gen.listOf(Gen.posNum[Int])

    val gen2:Gen[List[Int]] = for {
      l1 <- gen0
      l2 <- gen1
    } yield l1 ++ l2

    val decorator = new GenTraversableLikeDataGeneratorDecorator[Int]()

    val result = decorator.enrichGenData(gen0, gen1)

    val prop = result == gen2
    prop.apply(Gen.Parameters.default).success shouldBe true
  }

  "decorator" should "properly combine three generators" in {

    val gen0 = Gen.listOf(Gen.posNum[Int])
    val gen1 = Gen.listOf(Gen.posNum[Int])
    val gen2 = Gen.listOf(Gen.posNum[Int])

    val gen3:Gen[List[Int]] = for {
      l1 <- gen0
      l2 <- gen1
      l3 <- gen2
    } yield l1 ++ l2 ++ l3

    val decorator = new GenTraversableLikeDataGeneratorDecorator[Int]()

    val result = decorator.enrichGenData(gen0, gen1, gen2)

    val prop = result == gen3
    prop.apply(Gen.Parameters.default).success shouldBe true
  }
}
