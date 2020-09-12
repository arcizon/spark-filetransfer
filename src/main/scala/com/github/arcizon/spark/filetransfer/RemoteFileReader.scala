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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}

/**
  * Spark File Transfer '''DataFrameReader''' API class.
  *
  * @constructor Create a new `RemoteFileReader`.
  * @param sqlContext Current `'''SparkSession'''`'s SQL Context.
  * @param parameters Options specified on the '''DataFrameReader'''.
  * @param customSchema Optional schema to be used during file read.
  *
  * @since 0.1.0
  */
private[filetransfer] case class RemoteFileReader(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    customSchema: StructType
) extends BaseRelation
    with TableScan
    with Logging {

  private val options: FileTransferOptions = new FileTransferOptions(parameters)
  private val tempDir: File = FileUtils
    .createTempDir(
      root = options.localTempPath
    )
  private val dfsTempDir: Path = new Path(
    options.dfsTempPath,
    UUID.randomUUID().toString
  )

  private val df: DataFrame = {
    var dfr: DataFrameReader = sqlContext.read

    if (customSchema != null) {
      dfr = dfr.schema(customSchema)
    }

    fileTransferClient(options).download(options.path, tempDir.getCanonicalPath)
    val dfs: DfsUtils = new DfsUtils(sqlContext)
    dfs.copyFromLocal(tempDir.getCanonicalPath, dfsTempDir.toString)

    dfr
      .options(options.dfOptions)
      .format(options.fileFormat.toString)
      .load(dfsTempDir.toString)
  }

  override def schema(): StructType = {
    df.schema
  }

  override def buildScan(): RDD[Row] = {
    df.rdd
  }
}
