package com.github.arcizon.spark.filetransfer.util

import org.scalatest.FunSuite

class FileFormatTest extends FunSuite {

  test("jpeg is not allowed file format") {
    assertThrows[NoSuchElementException](FileFormat.withName("jpeg"))
  }

  test("File format string conversion check") {
    assert(FileFormat.avro == FileFormat.withName("avro"))
  }

}
