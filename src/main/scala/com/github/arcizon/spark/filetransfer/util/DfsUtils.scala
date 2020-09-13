package com.github.arcizon.spark.filetransfer.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext

/**
  * Utilities with the distributed file system operations.
  *
  * @since 0.1.0
  */
private[filetransfer] class DfsUtils(sqlContext: SQLContext) extends Logging {

  /**
    * Copies local file to distributed file path.
    *
    * @param src Local disk file path
    * @param dest Distributed file path
    *
    * @since 0.1.0
    */
  def copyFromLocal(src: String, dest: String): Unit = {
    log.info(s"Copying file from local path $src to dfs path $dest")
    val localPath: Path = new Path(src)
    val dfsPath: Path = new Path(dest)
    val fs = dfsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    fs.copyFromLocalFile(localPath, dfsPath)
    fs.deleteOnExit(dfsPath)
  }

  /**
    * Copies distributed file to local file path.
    *
    * @param src Distributed file path
    * @param dest Local disk file path
    *
    * @since 0.1.0
    */
  def copyToLocal(src: String, dest: String): Unit = {
    log.info(s"Copying file from dfs path $src to local path $dest")
    val dfsPath: Path = new Path(src)
    val localPath: Path = new Path(dest)
    val fs = dfsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    fs.copyToLocalFile(dfsPath, localPath)
    fs.deleteOnExit(dfsPath)
  }
}
