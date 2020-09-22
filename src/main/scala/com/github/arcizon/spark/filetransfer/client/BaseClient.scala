package com.github.arcizon.spark.filetransfer.client

import org.apache.spark.sql.SaveMode

/**
  * Factory for remote file transfer client.
  *
  * The following classes extends this trait:
  *  - [[com.github.arcizon.spark.filetransfer.client.SFTP SFTPClient]]
  *
  * @since 0.1.0
  */
trait BaseClient {

  /**
    * Uploads local files to remote host.
    *
    * @param src Local file/directory path.
    * @param dest Remote directory path.
    * @param mode Spark DataFrame Write Mode.
    * @return Returns unit of successful upload.
    *
    * @since 0.1.0
    */
  def upload(src: String, dest: String, mode: SaveMode): Unit

  /**
    * Downloads files from remote host.
    *
    * @param src Remote file/directory path.
    * @param dest Local directory path.
    * @return Returns unit of successful download.
    *
    * @since 0.1.0
    */
  def download(src: String, dest: String): Unit
}
