import java.util.Properties

object Application extends App {

  override def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("application.properties"))


//    val conf = new Configuration()
//    conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000")
//    conf.set("dfs.client.use.datanode.hostname", "true")
//
//    val fs = FileSystem.get(conf)
//    val path = new Path("/events")
//    fs.mkdirs(path)
//
//    val hDFSWriter = HDFSWriter(fs)
//    Gen.listOfN(10, new EventsGenerator().generateEvent())
//      .sample
//      .get
//      .map(EventToJsonSerializer.eventToJson)
//      .foreach(hDFSWriter.writeEventsToHDFS)
//
//    fs.close()
  }

}
