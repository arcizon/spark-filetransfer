package com.github.arcizon.spark.filetransfer.util

import org.scalatest.FunSuite

class ProtocolTest extends FunSuite {

  test("s3 is not allowed protocol") {
    assertThrows[NoSuchElementException](Protocol.withName("s3"))
  }

  test("Protocol string conversion check") {
    assert(Protocol.ftp == Protocol.withName("ftp"))
  }

}
