package com.griddynamics.generators

import java.io.OutputStream

import org.apache.hadoop.fs.FSDataOutputStream

case class CustomFSDataOutputStream(fsDataOutputStream: FSDataOutputStream) extends OutputStream {
  override def write(b: Int): Unit = fsDataOutputStream.write(b)

  override def write(b: Array[Byte]): Unit = fsDataOutputStream.write(b)

  override def write(b: Array[Byte], off: Int, len: Int): Unit = fsDataOutputStream.write(b, off, len)

  override def flush(): Unit = fsDataOutputStream.flush()

  override def close(): Unit = fsDataOutputStream.close()

  def writeUTF(string: String): Unit = fsDataOutputStream.writeUTF(string)
}
