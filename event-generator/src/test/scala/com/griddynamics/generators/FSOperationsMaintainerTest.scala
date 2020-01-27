package com.griddynamics.generators

import java.io.{FileSystem => _, _}
import java.net.URI

import org.apache.commons.io.input.ReaderInputStream
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.Try

class FSOperationsMaintainerTest extends FlatSpec with Matchers with MockFactory {

  sealed trait MockFileSystem extends FileSystem {
    override def getUri: URI = ???

    override def open(path: Path, i: Int): FSDataInputStream = ???

    override def create(path: Path, fsPermission: FsPermission, b: Boolean, i: Int, i1: Short, l: Long, progressable: Progressable): FSDataOutputStream = ???

    override def append(path: Path, i: Int, progressable: Progressable): FSDataOutputStream = ???

    override def rename(path: Path, path1: Path): Boolean = ???

    override def delete(path: Path, b: Boolean): Boolean = ???

    override def listStatus(path: Path): Array[FileStatus] = ???

    override def setWorkingDirectory(path: Path): Unit = ???

    override def getWorkingDirectory: Path = ???

    override def mkdirs(path: Path, fsPermission: FsPermission): Boolean = ???

    override def getFileStatus(path: Path): FileStatus = ???
  }

  "writer" should "flush data into hdfs" in new MockFileSystem {
    val mockCreate = mockFunction[Path, Boolean, FSDataOutputStream]
    val fSDataOutputStream = mock[FSDataOutputStream]
    val path = new Path("/")


    override def create(f: Path, overwrite: Boolean): FSDataOutputStream = mockCreate.apply(f, overwrite)


    mockCreate expects(path, false) returning fSDataOutputStream

    fSDataOutputStream.flush _ expects()
    fSDataOutputStream.close _ expects()

    FSOperationsMaintainer(this).writeToHDFS(path, "")
  }

  "writer" should "generate files with unique names" in new MockFileSystem {
    val size = Gen.posNum[Int].sample.get
    val paths = new ListBuffer[String]

    val maintainer = FSOperationsMaintainer(this)

    for (i <- 1 to size) {
      paths += maintainer.generateUniquePath.getName
    }

    assert(size === paths.distinct.size)
  }

  "outputStream" should "get closed in any case" in new MockFileSystem {
    val mockCreate = mockFunction[Path, Boolean, FSDataOutputStream]
    val fSDataOutputStream = mock[FSDataOutputStream]
    val path = new Path("/")

    override def create(f: Path, overwrite: Boolean): FSDataOutputStream = mockCreate.apply(f, overwrite)


    mockCreate expects(path, false) returning fSDataOutputStream

    fSDataOutputStream.flush _ expects() throwing new Exception()
    fSDataOutputStream.close _ expects()

    FSOperationsMaintainer(this).writeToHDFS(path, "")
  }


  "reader" should "properly read strings" in new MockFileSystem {
    val mockOpen = mockFunction[Path, FSDataInputStream]
    val bufferSize: Int = Gen.chooseNum(1, 4096).sample.get
    val str: String = Gen.alphaStr.sample.get

    override def open(f: Path): FSDataInputStream = mockOpen.apply(f)

    val fsDataInputStream = new FSDataInputStream(new BufferedFSInputStream(new ReaderInputStream(new StringReader(str))))

    mockOpen expects * returning fsDataInputStream

    val actualStr: Try[String] = FSOperationsMaintainer(this).readFile(new Path("/"), bufferSize)

    assert(str === actualStr.get)

  }
}

class BufferedFSInputStream(in: InputStream) extends BufferedInputStream(in: InputStream) with Seekable with PositionedReadable with HasFileDescriptor {


  override def getPos: Long = {
    if (this.in == null) {
      throw new IOException("Stream is closed!")
    }
    else {
      this.in.asInstanceOf[FSInputStream].getPos - (this.count - this.pos).toLong
    }
  }

  @throws[IOException]
  override def skip(n: Long): Long = {
    if (n <= 0L) {
      0L
    }
    else {
      this.seek(this.getPos + n)
      n
    }
  }

  @throws[IOException]
  override def seek(pos: Long): Unit = {
    if (this.in == null) {
      throw new IOException("Stream is closed!")
    }
    else {
      if (pos < 0L) {
        throw new IOException("Cannot seek to a negative offset")
      }
      else {
        if (this.pos != this.count) {
          val end: Long = this.in.asInstanceOf[FSInputStream].getPos
          val start: Long = end - this.count.toLong
          if (pos >= start && pos < end) {
            this.pos = (pos - start).toInt
            return
          }
        }
        this.pos = 0
        this.count = 0
        this.in.asInstanceOf[FSInputStream].seek(pos)
      }
    }
  }

  @throws[IOException]
  override def seekToNewSource(targetPos: Long): Boolean = {
    this.pos = 0
    this.count = 0
    this.in.asInstanceOf[FSInputStream].seekToNewSource(targetPos)
  }

  @throws[IOException]
  override def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
    this.in.asInstanceOf[FSInputStream].read(position, buffer, offset, length)
  }

  @throws[IOException]
  override def readFully(position: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = {
    this.in.asInstanceOf[FSInputStream].readFully(position, buffer, offset, length)
  }

  @throws[IOException]
  override def readFully(position: Long, buffer: Array[Byte]): Unit = {
    this.in.asInstanceOf[FSInputStream].readFully(position, buffer)
  }

  @throws[IOException]
  override def getFileDescriptor: FileDescriptor = {
    this.in match {
      case descriptor: HasFileDescriptor =>
        descriptor.getFileDescriptor
      case _ =>
        null
    }
  }
}
