package com.github.arcizon.spark.filetransfer.util

import java.io.{File, IOException}

import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
  * Factory for temporary directory cleanup created
  * to store intermediate upload/download content.
  *
  * @since 0.1.0
  */
private[util] object Cleanup extends Logging {

  /**
    * HashSet storing the paths to delete on JVM Shutdown Hook call.
    *
    * @since 0.1.0
    */
  val shutdownDeletePaths = new mutable.HashSet[String]()

  // Add a shutdown hook to delete the temp dirs when the JVM exits
  Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dirs") {
    override def run(): Unit = {
      shutDownCleanUp()
    }
  })

  /**
    * Cleanup method called from the JVM Shutdown Hook.
    *
    * @since 0.1.0
    */
  def shutDownCleanUp(): Unit = {
    shutdownDeletePaths.foreach(cleanupPath)
  }

  /**
    * Registers the local path to cleanup JVM Shutdown Hook.
    *
    * @param file Path to delete as the part of cleanup.
    *
    * @since 0.1.0
    */
  def registerShutdownDeleteDir(file: File) {
    val path = file.getAbsolutePath
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += path
    }
  }

  /**
    * Cleans up the provided directory path recursively.
    *
    * @param dirPath Directory Path to cleanup.
    *
    * @since 0.1.0
    */
  private def cleanupPath(dirPath: String): Unit = {
    try {
      deleteRecursively(new File(dirPath))
    } catch {
      // Doesn't really matter if we fail.
      // $COVERAGE-OFF$
      case _: Exception => log.warn("Exception during cleanup !!")
      // $COVERAGE-ON$
    }
  }

  /**
    * Recursively deletes the provided path.
    * Don't follow directories if they are symlinks.
    *
    * @param file Path to recursively delete.
    *
    * @throws java.io.IOException Throws an exception if deletion is unsuccessful.
    *
    * @since 0.1.0
    */
  @throws[IOException]
  private def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            // $COVERAGE-OFF$
            throw savedIOException
            // $COVERAGE-ON$
          }
          shutdownDeletePaths.synchronized {
            shutdownDeletePaths.remove(file.getAbsolutePath)
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          // $COVERAGE-OFF$
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
          // $COVERAGE-ON$
        }
      }
    }
  }

  /**
    * Checks if the provided path is a symbolic link.
    *
    * @param file Path to check for symbolic link.
    * @return True if the path is symbolic link, otherwise False.
    *
    * @since 0.1.0
    */
  private def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    val fileInCanonicalDir = if (file.getParent == null) {
      file
    } else {
      new File(file.getParentFile.getCanonicalFile, file.getName)
    }

    !fileInCanonicalDir.getCanonicalFile.equals(
      fileInCanonicalDir.getAbsoluteFile
    )
  }

  /**
    * Retrieves the list of files present in the provided path.
    *
    * @param file Path to list files from.
    * @return Sequence of files on the provided path.
    *
    * @since 0.1.0
    */
  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }
}
