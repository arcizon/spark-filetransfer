package com.github.arcizon.spark.filetransfer

import com.github.arcizon.spark.filetransfer.testfactory.{
  IntegrationTest,
  SparkFactory
}
import com.holdenkarau.spark.testing.HDFSClusterLike
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DefaultSourceTest
    extends FunSuite
    with HDFSClusterLike
    with SparkFactory
    with BeforeAndAfterAll {

  private var hdfsTempPath: Path = _

  override def beforeAll(): Unit = {
    super.startHDFS()
    hdfsTempPath = new Path(super.getNameNodeURI(), "/tests")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    super.shutdownHDFS()
  }

  test(
    "Reading remote dataset via this datasource on classpath",
    IntegrationTest
  ) {
    val readOptions: Map[String, String] = Map(
      "host" -> "localhost",
      "port" -> "2222",
      "username" -> "foo",
      "password" -> "pass",
      "fileFormat" -> "csv",
      "dfsTempPath" -> hdfsTempPath.toString,
      "delimiter" -> ",",
      "header" -> "true"
    )
    val df: DataFrame = spark.read
      .options(readOptions)
      .sftp("data/sparkdata/iris.csv")
    assert(df.count === 150)
  }

  test(
    "Writing dataset to remote host via this datasource on classpath",
    IntegrationTest
  ) {
    val data: Seq[Row] = Seq(
      Row("IST", "Indian Standard Time"),
      Row("GMT", "Greenwich Mean Time"),
      Row("EST", "Eastern Standard Time")
    )

    val schema: StructType = StructType.fromDDL(
      "tz string, name string"
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val writeOptions: Map[String, String] = Map(
      "host" -> "localhost",
      "port" -> "2222",
      "username" -> "foo",
      "password" -> "pass",
      "fileFormat" -> "json",
      "dfsTempPath" -> hdfsTempPath.toString,
      "multiLine" -> "true"
    )
    val dfw: DataFrameWriter[Row] = df
      .coalesce(1)
      .sort(df("tz"))
      .write
      .format(this.getClass.getPackage.getName)
      .mode(SaveMode.Append)
      .options(writeOptions)

    (1 to 2).foreach(i => {
      log.info(s"Running DF write Append operation $i out of 2 times")
      dfw.sftp(s"data/upload/append")
    })
  }

}
