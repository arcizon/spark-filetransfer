package com.github.arcizon.spark.filetransfer

import java.io.File

import com.github.arcizon.spark.filetransfer.client.fileTransferClient
import com.github.arcizon.spark.filetransfer.util.{
  FileTransferOptions,
  FileUtils
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{
  DataFrame,
  DataFrameWriter,
  Row,
  SQLContext,
  SaveMode
}

/**
  * Spark File Transfer '''DataFrameWriter''' API class.
  *
  * @constructor Create a new `RemoteFileWriter`.
  * @param mode Save mode to write the Spark '''DataFrame''' onto the storage.
  * @param sqlContext Current `'''SparkSession'''`'s SQL Context.
  * @param parameters Options specified on the '''DataFrameWriter'''.
  * @param data Loaded Spark '''DataFrame'''.
  *
  * @since 0.1.0
  */
private[filetransfer] case class RemoteFileWriter(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame
) extends BaseRelation
    with Logging {

  override def schema: StructType = data.schema

  private val options: FileTransferOptions = new FileTransferOptions(parameters)
  private val tempDir: String = FileUtils
    .createTempDir(
      root = options.localTempPath
    )
    .getCanonicalPath

  private val dfw: DataFrameWriter[Row] = data.write

  dfw
    .options(options.dfOptions)
    .format(options.fileFormat.toString)
    .mode(mode)
    .save(tempDir)

  private val uploadPath: String = FileUtils.collectUploadFiles(tempDir)
  fileTransferClient(options).upload(uploadPath, options.path)
}
