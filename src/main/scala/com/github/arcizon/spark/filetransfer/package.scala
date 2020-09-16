package com.github.arcizon.spark

import com.github.arcizon.spark.filetransfer.util.{
  FileTransferOptions,
  Protocol
}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object filetransfer {

  /**
    * Adds a method, `xml`, to DataFrameReader that allows you to read avro files using
    * the DataFileReader
    */
  implicit class ProtocolDataFrameReader(reader: DataFrameReader) {
    def sftp: String => DataFrame = {
      reader
        .format("com.github.arcizon.spark")
        .option(FileTransferOptions.PROTOCOL, Protocol.sftp.toString)
        .load
    }
  }

  /**
    * Adds a method, `xml`, to DataFrameWriter that allows you to write avro files using
    * the DataFileWriter
    */
  implicit class ProtocolDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def sftp: String => Unit = {
      writer
        .format("com.github.arcizon.spark")
        .option(FileTransferOptions.PROTOCOL, Protocol.sftp.toString)
        .save
    }
  }

}
