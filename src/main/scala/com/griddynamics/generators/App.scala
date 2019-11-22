package com.griddynamics.generators

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalacheck.Gen

object App {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://127.0.0.1:39000")
//    conf.set("minReplication", "0")
    val fs = FileSystem.get(conf)
    val hDFSWriter = HDFSWriter(fs)

    Gen.infiniteStream(new EventsGenerator().generateEvent())
      .sample
      .get
      .map(EventToJsonSerializer.eventToJson)
      .foreach(hDFSWriter.writeEventsToHDFS)
  }

}
