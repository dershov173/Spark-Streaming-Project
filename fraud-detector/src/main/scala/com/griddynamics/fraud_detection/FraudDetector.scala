package com.griddynamics.fraud_detection

import org.apache.spark.sql.SparkSession

object FraudDetector extends org.apache.spark.deploy.SparkHadoopUtil {
  //  val kafkaParams = Map[String, Object](
  //    "bootstrap.servers" -> "localhost:9092",
  //    "key.deserializer" -> classOf[EventIdDeserializer],
  //    "value.deserializer" -> classOf[EventFromJsonDeserializer],
  //    "group.id" -> "g1",
  //    "auto.offset.reset" -> "earliest",
  //    "enable.auto.commit" -> (false: java.lang.Boolean)
  //  )
  //

  def main(args: Array[String]): Unit = {


    val sparkSession: SparkSession = SparkSession
      .builder
      .appName("fraud_detector")
      .master("local[4]")
      //      .config("jaasddasrs", "/Users/dershov/.m2/repository/org/apache/spark/spark-streaming-kafka-0-10_2.12/2.4.0/spark-streaming-kafka-0-10_2.12-2.4.0.jar")
      //      .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.0")
      //          .config("packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12")
      .getOrCreate()

    //    sparkSession.conf.set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.0")
    println(sparkSession.conf.getAll.toString)

    import sparkSession.implicits._
    sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "events")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      //      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()


  }


  //
  //  private val streamingContext = new StreamingContext(sparkConf, Duration.apply(100L))
  //  val topics = Array("events")
  //  val stream: InputDStream[ConsumerRecord[EventIdentifier, Event]] = KafkaUtils.createDirectStream[EventIdentifier, Event](
  //    streamingContext,
  //    PreferConsistent,
  //    Subscribe[EventIdentifier, Event](topics, kafkaParams)
  //  )
  //
  //  stream.count().print()
  //
  //  streamingContext.start()
  //  streamingContext.awaitTermination()

}
