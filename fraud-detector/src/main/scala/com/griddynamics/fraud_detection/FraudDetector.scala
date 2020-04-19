package com.griddynamics.fraud_detection

import com.griddynamics.generators.{Event, EventFromJsonDeserializer}
import org.apache.spark.sql.{Row, SparkSession}

object FraudDetector extends org.apache.spark.deploy.SparkHadoopUtil {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    implicit val sparkSession: SparkSession = SparkSession
      .builder
      .appName("fraud_detector")
      .master("local[4]")
      .config("spark.streaming.stopGracefullyOnShutdown", value = true)
      //      .config("jaasddasrs", "/Users/dershov/.m2/repository/org/apache/spark/spark-streaming-kafka-0-10_2.12/2.4.0/spark-streaming-kafka-0-10_2.12-2.4.0.jar")
      //      .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.0")
      //          .config("packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12")
      .getOrCreate()

    import sparkSession.implicits._
    val events = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .option("spark.streaming.stopGracefullyOnShutdown", value = true)
      .load()
      .map { row =>
        EventFromJsonDeserializer.deserialize(row.getString(0)).get
      }


    val query = BotsIdentifier(windowDuration = "20 seconds").identifyBots(events)
      .writeStream
      .outputMode("complete")
//              .format("memory")
//              .queryName("tableName")
      .format("console")
      .start()

    def runStream() : Unit = {
        query.awaitTermination()
    }

    runStream()
//    val thread = new Thread(() => runStream())
//    thread.start()
//    Thread.sleep(20000)
//    query.stop()
//    thread.join()




//    thread.interrupt()

//    sparkSession.table("tableName").show()
//    sparkSession
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
//      .option("subscribe", "events")
//      .load()
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//      .writeStream
//      //      .outputMode("complete")
//      .format("console")
//      .start()
//      .awaitTermination()
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


