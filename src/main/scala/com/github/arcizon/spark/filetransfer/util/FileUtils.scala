package com.github.arcizon.spark.filetransfer.util

import java.io.{File, IOException}
import java.util.UUID

import scala.util.Try

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
    * like `<prefix>-0` into a parts directory.
    *
    * @param path Local Path to spark `DataFrame` write output.
    * @param prefix Prefix for the upload file name.
    * @return Canonical Path to local output files to upload to remote host.
    *
    * @since 0.1.0
    */
  def collectUploadFiles(path: String, prefix: String): String = {
    val srcPath: File = new File(path)
    val files = srcPath.listFiles().filter { x =>
      (!x.isDirectory
      && !x.getName.contains("SUCCESS")
      && !x.isHidden
      && !x.getName.contains(".crc")
      && !x.getName.contains("_committed_")
      && !x.getName.contains("_started_"))
    }
    val headFile: File = files.head
    val ext: String = getFileExt(headFile)
    val partsDir = new File(srcPath, "parts")
    partsDir.mkdir()
    for (i <- files.indices) {
      files(i).renameTo(new File(partsDir, s"$prefix-$i.$ext"))
    }
    partsDir.getCanonicalPath
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

  /**
    * Extracts file extension from the provided file.
    *
    * @param file A file with possible extension to be extracted
    * @return The file extension
    *
    * @since 0.1.0
    */
  @throws[RuntimeException]
  def getFileExt(file: File): String = {
    Try(file.getName.split("\\.", 2)(1))
      .getOrElse(
        sys.error(
          "Unable to extract the extension of the file to be uploaded!!"
        )
      )
  }
}
