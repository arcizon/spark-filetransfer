package com.github.arcizon.spark

import com.github.arcizon.spark.filetransfer.util.{
  FileTransferOptions,
  Protocol
}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object filetransfer {

  /**
    * Adds methods for each supported [[com.github.arcizon.spark.filetransfer.util.Protocol Protocol]],
    * to DataFrameReader that allows you to read remote files via the selected protocol.
    */
  implicit class ProtocolDataFrameReader(reader: DataFrameReader) {
    def sftp: String => DataFrame = {
      reader
        .format("com.github.arcizon.spark.filetransfer")
        .option(FileTransferOptions.PROTOCOL, Protocol.sftp.toString)
        .load
    }
  }

  /**
    * Adds methods for each supported [[com.github.arcizon.spark.filetransfer.util.Protocol Protocol]],
    * to DataFrameWriter that allows you to write remote files via the selected protocol.
    */
  implicit class ProtocolDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def sftp: String => Unit = {
      writer
        .format("com.github.arcizon.spark.filetransfer")
        .option(FileTransferOptions.PROTOCOL, Protocol.sftp.toString)
        .save
    }
  }

}
