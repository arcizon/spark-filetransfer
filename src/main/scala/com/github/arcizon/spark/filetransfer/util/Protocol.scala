package com.github.arcizon.spark.filetransfer.util

/**
  * Supported file transfer protocols to upload/download content.
  *
  *  1. [[com.github.arcizon.spark.filetransfer.util.Protocol#ftp ftp]]
  *  1. [[com.github.arcizon.spark.filetransfer.util.Protocol#scp scp]]
  *  1. [[com.github.arcizon.spark.filetransfer.util.Protocol#sftp sftp]]
  *
  * @since 0.1.0
  */
object Protocol extends Enumeration {
  type Protocol = Value
  val ftp, scp, sftp = Value
}
