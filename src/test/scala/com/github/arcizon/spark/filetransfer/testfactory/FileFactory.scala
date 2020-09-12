package com.github.arcizon.spark.filetransfer.testfactory

import java.io.File
import java.util.UUID

import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.reflect.io.Directory

trait FileFactory extends BeforeAndAfterEach {
  this: Suite =>

  val tmpDir: File = new File(
    System.getProperty("java.io.tmpdir", "/tmp"),
    UUID.randomUUID().toString
  )

  var file: File = tmpDir

  override def beforeEach(): Unit = {
    file.mkdirs()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (file.exists()) {
      if (file.isDirectory) {
        val dir: Directory = new Directory(file)
        dir.deleteRecursively()
      } else {
        file.delete()
      }
    }
  }

}
