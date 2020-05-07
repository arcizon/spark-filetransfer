package com.github.arcizon.spark.filetransfer.util

import java.io.File

import com.github.arcizon.spark.filetransfer.testfactory.FileFactory
import org.scalatest.FunSuite

class CleanupTest extends FunSuite with FileFactory {

  test("Register directory to shutdown hook for deletion on exit") {
    Cleanup.registerShutdownDeleteDir(file)
    for (i <- 1 to 5) {
      val tempDir: File = new File(file, s"$i")
      tempDir.mkdir()
      new File(tempDir, s"dummy-$i.txt").createNewFile()
    }
    assert(Cleanup.shutdownDeletePaths.contains(file.getAbsolutePath))
    assertResult(())(Cleanup.shutDownCleanUp())
  }

  test("Run directory cleanup") {
    Cleanup.registerShutdownDeleteDir(file)
    Cleanup.shutDownCleanUp()
    assert(!file.exists())
  }

  test("Prior deletion of registered shutdown hook delete directory") {
    val temp: File = new File(file, "cleantmp-")
    temp.mkdir()
    Cleanup.registerShutdownDeleteDir(temp)
    temp.delete()
    assertResult(())(Cleanup.shutDownCleanUp())
  }
}
