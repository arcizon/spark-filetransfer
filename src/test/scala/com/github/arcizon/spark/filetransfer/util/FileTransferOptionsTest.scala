package com.github.arcizon.spark.filetransfer.util

import com.github.arcizon.spark.filetransfer.testfactory.FileTransferOptionsFactory
import org.scalatest.FunSuite

class FileTransferOptionsTest extends FunSuite with FileTransferOptionsFactory {

  import FileTransferOptions._

  test("Default options check") {
    val fto = new FileTransferOptions(params)
    assert(fto.host === "localhost")
    assert(fto.port === 22)
    assert(fto.protocol === Protocol.sftp)
    assert(fto.keyFilePath === None)
    assert(fto.dfOptions === Map("delimiter" -> "|"))
  }

  test("Username option is required") {
    lazy val fto = new FileTransferOptions(
      params.filterKeys(!_.equals(USERNAME))
    )
    val caught = intercept[RuntimeException](fto)
    assert(caught.getMessage === s"Missing '$USERNAME' option!!")
  }

  test("File format option is required") {
    lazy val fto = new FileTransferOptions(
      params.filterKeys(!_.equals(FILE_FORMAT))
    )
    val caught = intercept[RuntimeException](fto)
    assert(caught.getMessage startsWith s"Missing '$FILE_FORMAT' option")
  }

  test("Path option is required") {
    lazy val fto = new FileTransferOptions(
      params.filterKeys(!_.equals(PATH))
    )
    val caught = intercept[RuntimeException](fto)
    assert(caught.getMessage === s"Missing '$PATH' option!!")
  }

  test("No password or key supplied") {
    lazy val fto = new FileTransferOptions(
      params.filterKeys(!_.equals(PASSWORD))
    )
    val caught = intercept[IllegalArgumentException](fto)
    assert(
      caught.getMessage.contains(
        s"Either '$KEY_FILE_PATH' or '$PASSWORD' is required to establish remote connection!!"
      )
    )
  }

  test("Both password and key supplied") {
    params += (KEY_FILE_PATH -> "~/.ssh/test_rsa")
    lazy val fto = new FileTransferOptions(params)
    val caught = intercept[IllegalArgumentException](fto)
    assert(
      caught.getMessage.contains(
        s"Either '$KEY_FILE_PATH' or '$PASSWORD' is required to establish remote connection!!"
      )
    )
  }

}
