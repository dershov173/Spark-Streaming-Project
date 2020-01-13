package com.griddynamics.kafka.connector

import java.util

import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

class HDFSKafkaSourceTask extends SourceTask {
  override def start(props: util.Map[String, String]): Unit = ???

  override def poll(): util.List[SourceRecord] = ???

  override def stop(): Unit = ???

  override def version(): String = VersionUtil.getVersion
}
