package com.github.arcizon.spark.filetransfer

import java.io.File
import java.util.UUID

import com.github.arcizon.spark.filetransfer.client.fileTransferClient
import com.github.arcizon.spark.filetransfer.util.{
  DfsUtils,
  FileTransferOptions,
  FileUtils
}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

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
  private val tempDir: File = FileUtils
    .createTempDir(
      root = options.localTempPath
    )
  private val dfsTempDir: Path = new Path(
    options.dfsTempPath,
    UUID.randomUUID().toString
  )

  private val dfw: DataFrameWriter[Row] = data.write

  dfw
    .options(options.dfOptions)
    .format(options.fileFormat.toString)
    .mode(mode)
    .save(dfsTempDir.toString)

  val dfs: DfsUtils = new DfsUtils(sqlContext)
  dfs.copyToLocal(dfsTempDir.toString, tempDir.getCanonicalPath)
  private val uploadPath: String = FileUtils.collectUploadFiles(
    new File(tempDir, dfsTempDir.getName).getCanonicalPath
  )
  fileTransferClient(options).upload(uploadPath, options.path)
}
