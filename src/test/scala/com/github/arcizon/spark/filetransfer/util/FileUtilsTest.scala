package com.github.arcizon.spark.filetransfer.util

import java.io.File

import com.github.arcizon.spark.filetransfer.testfactory.FileFactory
import org.scalatest.FunSuite

class FileUtilsTest extends FunSuite with FileFactory {

  test("File creation") {
    file = new File(tmpDir, "dummy.txt")
    FileUtils.touch(file.getAbsolutePath)
    assert(file.exists())
  }

  test("Error on existing file") {
    file = new File(tmpDir, "error.txt")
    file.createNewFile()
    val caught = intercept[RuntimeException] {
      FileUtils.touch(file.getAbsolutePath)
    }
    val expectedMsg: String =
      s"Path '${file.getAbsolutePath}' already exists!!"
    assert(caught.getMessage === expectedMsg)
  }

  test("Collect Spark Part files") {
    file = new File(tmpDir, "multiple")
    file.mkdirs()
    val outFiles: List[String] = List(
      "_SUCCESS",
      "part-000-4545-556.txt",
      "part-001-4553-554.txt"
    )
    outFiles.foreach(x => new File(file, x).createNewFile())
    val upload: String =
      FileUtils.collectUploadFiles(file.getAbsolutePath, "test")
    val parts: List[String] = new File(upload)
      .listFiles()
      .toList
      .map(
        _.getName
      )
    assert(parts.sorted == List("test-0.txt", "test-1.txt"))
  }

  test("Creating temporary directory") {
    val temp: File = FileUtils.createTempDir(file.getAbsolutePath)
    assert(temp.getName.startsWith("spark-filetransfer-"))
  }

  test("Unable to extract file extension") {
    assertThrows[RuntimeException](
      FileUtils.getFileExt(new File("test/sample"))
    )
  }
}
