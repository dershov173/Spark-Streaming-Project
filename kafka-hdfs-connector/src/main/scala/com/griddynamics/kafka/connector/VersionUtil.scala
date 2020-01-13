package com.griddynamics.kafka.connector

object VersionUtil {
  def getVersion: String = try
    this.getClass.getPackage.getImplementationVersion
  catch {
    case ex: Exception =>
      "0.0.0.0"
  }
}
