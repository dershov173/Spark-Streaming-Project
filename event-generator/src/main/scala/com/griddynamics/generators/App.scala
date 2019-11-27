package com.griddynamics.generators

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalacheck.Gen

object App {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000")
    conf.set("dfs.client.use.datanode.hostname", "true")

    val fs = FileSystem.get(conf)
    val path = new Path("/events")
    fs.mkdirs(path)

    val hDFSWriter = HDFSWriter(fs)



    Gen.listOfN(10, new EventsGenerator().generateEvent())
      .sample
      .get
      .map(EventToJsonSerializer.eventToJson)
      .foreach(hDFSWriter.writeEventsToHDFS)
  }

}
