import java.util.Properties

import com.griddynamics.generators.{EventsWriter, PropertiesWrapper}
import org.slf4j.{Logger, LoggerFactory}

object Application extends App {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("application.properties"))

    val eventsWriter = EventsWriter(PropertiesWrapper(properties))
    try {
      eventsWriter.writeEvents()
    } catch {
      case t: Throwable => logger.error("There was an error occurred when trying to  generate events and write " +
        "them to HDFS", t)
    } finally {
      eventsWriter.close()
    }
  }

}
