package com.github.arcizon.spark.filetransfer

import java.util.UUID

import com.github.arcizon.spark.filetransfer.testfactory.{
  IntegrationTest,
  SparkFactory
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RemoteFileWriterTest
    extends FunSuite
    with SparkFactory
    with BeforeAndAfterAll {

  private var params: Map[String, String] = _
  private var customSchema: StructType = _
  private val uploadDir: String = s"upload/${UUID.randomUUID().toString}"

  override def beforeAll(): Unit = {
    params = Map(
      "host" -> "localhost",
      "port" -> "2222",
      "username" -> "foo",
      "password" -> "pass",
      "fileFormat" -> "parquet",
      "path" -> s"data/$uploadDir/",
      "mergeSchema" -> "true"
    )
    customSchema = StructType.fromDDL(
      "id int, SepalLength double, SepalWidth double, PetalLength double, PetalWidth double, Species string"
    )
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    params = Map.empty
  }

  test("Write dataset to remote host", IntegrationTest) {
    val srcFile: String =
      this.getClass.getResource("/sftp/sparkdata/iris.csv").getPath
    val df: DataFrame = spark.read
      .option("header", "true")
      .schema(customSchema)
      .csv(srcFile)
    val rfw: RemoteFileWriter = RemoteFileWriter(
      spark.sqlContext,
      SaveMode.Overwrite,
      params,
      df
    )
    assert(rfw.schema === customSchema)
  }

}
