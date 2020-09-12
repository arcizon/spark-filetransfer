package com.github.arcizon.spark.filetransfer.util

import java.io.{File, IOException}
import java.util.UUID

/**
  * Utilities with the file system operations.
  *
  * @since 0.1.0
  */
private[filetransfer] object FileUtils {

  /**
    * Creates an empty file on the local file system.
    *
    * @param path Path to create an empty file.
    *
    * @since 0.1.0
    */
  def touch(path: String): Unit = {
    val destPath: File = new File(path)
    if (destPath.exists()) {
      sys.error(s"Path '$path' already exists!!")
    }
    val parentPath = destPath.getCanonicalFile
    if (!parentPath.exists()) parentPath.mkdirs()
    destPath.createNewFile()
  }

  /**
    * Collects all the part files from the Spark Dataframe by
    * filtering out the flag files created by the
    * [[org.apache.spark.sql.DataFrameWriter DataFrameWriter]]
    * and moves the part files by renaming them with index
    * like `part-1` into a parts directory if more than one
    * file is found.
    *
    * @param path Local Path to spark `DataFrame` write output.
    * @return Canonical Path to local output files to upload to remote host.
    *
    * @since 0.1.0
    */
  def collectUploadFiles(path: String): String = {
    val srcPath: File = new File(path)
    val files = srcPath.listFiles().filter { x =>
      (!x.isDirectory
      && !x.getName.contains("SUCCESS")
      && !x.isHidden
      && !x.getName.contains(".crc")
      && !x.getName.contains("_committed_")
      && !x.getName.contains("_started_"))
    }
    if (files.length > 1) {
      val partsDir = new File(srcPath, "parts")
      partsDir.mkdir()
      val ext: String = files.head.getName.split("\\.", 2)(1)
      for (i <- files.indices) {
        files(i).renameTo(new File(partsDir, s"part-$i.$ext"))
      }
      partsDir.getCanonicalPath
    } else {
      files(0).getCanonicalPath
    }
  }

  /**
    * Creates a local temporary directory and registers it to
    * get deleted on the JVM shutdown.
    *
    * @param root Path to generate a temporary directory.
    * @return Path to the generated temporary directory.
    *
    * @since 0.1.0
    */
  def createTempDir(root: String): File = {
    val dir: File = createTempDirectory(root)
    Cleanup.registerShutdownDeleteDir(dir)
    dir
  }

  /**
    * Creates a local temporary directory.
    *
    * @param root Path to generate a temporary directory.
    * @return Path to the generated temporary directory.
    *
    * @since 0.1.0
    */
  private def createTempDirectory(root: String): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException(
          s"Failed to create a temp directory (under $root) after $maxAttempts"
        )
      }
      try {
        dir = new File(root, "spark-filetransfer-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch {
        case _: SecurityException => dir = null
      }
    }

    dir
  }
}
