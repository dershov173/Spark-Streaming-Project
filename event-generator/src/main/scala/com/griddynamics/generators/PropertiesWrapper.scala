package com.griddynamics.generators

import java.util.Properties

case class PropertiesWrapper(properties:Properties) {
  def getProperty(s:String): String = properties.getProperty(s)
  def getOrDefaultString(name:String, default:String): String =
    properties.getProperty(name, default)

  def getIntProperty(name:String, defaultValue: Int): Int = {
    Option(properties.getProperty(name))
      .map(Integer.parseInt)
      .getOrElse(defaultValue)
  }

  def getLongProperty(name:String, defaultValue: Long): Long = {
    Option(properties.getProperty(name))
      .map(java.lang.Long.parseLong)
      .getOrElse(defaultValue)
  }

  def getBooleanProperty(name:String, defaultValue: Boolean): Boolean = {
    Option(properties.getProperty(name))
      .map(java.lang.Boolean.parseBoolean)
      .getOrElse(defaultValue)
  }
}
