package com.github.arcizon.spark.filetransfer.testfactory

import org.scalatest.{BeforeAndAfterEach, Suite}

trait FileTransferOptionsFactory extends BeforeAndAfterEach {
  this: Suite =>

  var params: Map[String, String] = _

  override def beforeEach(): Unit = {
    params = Map(
      "username" -> "test",
      "password" -> "dummy",
      "fileFormat" -> "csv",
      "path" -> "/tmp",
      "delimiter" -> "|"
    )
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    params = Map.empty
  }
}
