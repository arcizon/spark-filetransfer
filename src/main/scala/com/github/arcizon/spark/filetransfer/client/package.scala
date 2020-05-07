package com.github.arcizon.spark.filetransfer

import com.github.arcizon.spark.filetransfer.util.{
  FileTransferOptions,
  Protocol
}

/**
  * Package object for common methods to access client classes.
  *
  * @since 0.1.0
  */
package object client {

  /**
    * Creates a ''client'' instance to the remote host for file transfer
    * for the type of the
    * [[com.github.arcizon.spark.filetransfer.util.Protocol.Value Protocol]]
    * value set in the options.
    *
    * @param options Options passed by the user on the
    *                '''DataFrameReader''' or '''DataFrameWriter''' API.
    * @return A file transfer client instance.
    * @see [[com.github.arcizon.spark.filetransfer.client.SFTP SFTPClient]]
    * @since 0.1.0
    */
  def fileTransferClient(
      options: FileTransferOptions
  ): BaseClient = {
    options.protocol match {
      case Protocol.sftp => new SFTP(options)
      case x @ _ =>
        throw new NotImplementedError(
          s"Support ${x.toString} client is not yet implemented !!"
        )
    }
  }
}
