package com.github.arcizon.spark.filetransfer.client

import com.github.arcizon.spark.filetransfer.testfactory.FileTransferOptionsFactory
import com.github.arcizon.spark.filetransfer.util.FileTransferOptions
import org.scalatest.FunSuite

class ClientTest extends FunSuite with FileTransferOptionsFactory {

  test("Picking SFTP client") {
    val fto: FileTransferOptions = new FileTransferOptions(params)
    assert(fileTransferClient(fto).isInstanceOf[SFTP])
  }

  test("Picking unimplemented client") {
    params += ("protocol" -> "ftp")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    assertThrows[NotImplementedError](fileTransferClient(fto))
  }
}
