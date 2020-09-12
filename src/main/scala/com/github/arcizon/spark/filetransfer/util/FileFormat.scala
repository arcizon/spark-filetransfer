package com.github.arcizon.spark.filetransfer.util

/**
  * Supported file formats to read/write data via Spark '''DataFrame API'''.
  *
  *  1. [[com.github.arcizon.spark.filetransfer.util.FileFormat#avro avro]]
  *  1. [[com.github.arcizon.spark.filetransfer.util.FileFormat#csv csv]]
  *  1. [[com.github.arcizon.spark.filetransfer.util.FileFormat#json json]]
  *  1. [[com.github.arcizon.spark.filetransfer.util.FileFormat#orc orc]]
  *  1. [[com.github.arcizon.spark.filetransfer.util.FileFormat#parquet parquet]]
  *  1. [[com.github.arcizon.spark.filetransfer.util.FileFormat#text text]]
  *  1. [[com.github.arcizon.spark.filetransfer.util.FileFormat#xml xml]]
  *
  * @since 0.1.0
  */
object FileFormat extends Enumeration {
  type FileFormat = Value
  val avro, csv, json, orc, parquet, text, xml = Value
}
