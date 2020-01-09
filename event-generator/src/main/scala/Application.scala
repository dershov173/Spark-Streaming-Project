import java.util.Properties

import com.griddynamics.generators.{EventsWriter, PropertiesWrapper}

object Application extends App {

  override def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("application.properties"))

    EventsWriter(PropertiesWrapper(properties)).writeEvents()
  }

}
