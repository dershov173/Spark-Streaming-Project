package com.griddynamics.fraud_detection

import org.apache.spark.sql.Dataset
import org.scalacheck.Gen


package object decorators {
  trait DataGeneratorDecorator[T] {
    def enrichGenData(gen0 :Gen[T], generators: Gen[T]*) : Gen[T]
  }

  class GenTraversableLikeDataGeneratorDecorator[E]
    extends DataGeneratorDecorator[Seq[E]] {
    override def enrichGenData(gen0: Gen[Seq[E]],
                               generators: Gen[Seq[E]]*): Gen[Seq[E]] = {
      generators.foldLeft(gen0)((g1, g2) => {
        for {
          head <- g1
          tail <- g2
        } yield head ++ tail
      })
    }

  }

  class GenDatasetGeneratorDecorator[E]
    extends DataGeneratorDecorator[Dataset[E]] {
    override def enrichGenData(gen0: Gen[Dataset[E]],
                               generators: Gen[Dataset[E]]*): Gen[Dataset[E]] = {
      generators.foldLeft(gen0)((g1, g2) => {
        for {
          head <- g1
          tail <- g2
        } yield head union tail
      })
    }
  }

}
