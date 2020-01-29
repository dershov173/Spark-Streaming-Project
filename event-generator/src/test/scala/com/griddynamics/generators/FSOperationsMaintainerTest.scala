package com.griddynamics.generators

import java.io.{FileSystem => _}
import java.net.URI

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

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
    val mockCreate = mockFunction[Path, Boolean, CustomFSDataOutputStream]
    val fSDataOutputStream = mock[CustomFSDataOutputStream]
    val path = new Path("/")
    val content = Gen.alphaStr.sample.get

    mockCreate expects(path, false) returning fSDataOutputStream
    fSDataOutputStream.writeUTF _ expects content
    fSDataOutputStream.flush _ expects()
    fSDataOutputStream.close _ expects()

    val operationsMaintainer: FSOperationsMaintainer = new FSOperationsMaintainer(this) {
      override def create(p: Path, overwrite: Boolean): CustomFSDataOutputStream = mockCreate.apply(p, overwrite)
    }

    operationsMaintainer.writeToHDFS(path, content)
  }

  "writer" should "generate files with unique names" in new MockFileSystem {
    val size = Gen.posNum[Int].sample.get
    val paths = new ListBuffer[String]

    val maintainer = FSOperationsMaintainer(this)

    for (_ <- 1 to size) {
      paths += maintainer.generateUniquePath.getName
    }

    assert(size === paths.distinct.size)
  }

  "outputStream" should "get closed in any case" in new MockFileSystem {
    val mockCreate = mockFunction[Path, Boolean, CustomFSDataOutputStream]
    val fSDataOutputStream = mock[CustomFSDataOutputStream]
    val path = new Path("/")
    val content = Gen.alphaStr.sample.get

    mockCreate expects(path, false) returning fSDataOutputStream
    fSDataOutputStream.writeUTF _ expects content
    fSDataOutputStream.flush _ expects() throwing new Exception()
    fSDataOutputStream.close _ expects()

    val operationsMaintainer: FSOperationsMaintainer = new FSOperationsMaintainer(this) {
      override def create(p: Path, overwrite: Boolean): CustomFSDataOutputStream = mockCreate.apply(p, overwrite)
    }

    operationsMaintainer.writeToHDFS(path, content)
  }
}
