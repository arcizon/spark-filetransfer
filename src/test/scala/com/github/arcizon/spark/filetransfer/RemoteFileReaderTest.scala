package com.github.arcizon.spark.filetransfer

import com.github.arcizon.spark.filetransfer.testfactory.{
  IntegrationTest,
  SparkFactory
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RemoteFileReaderTest
    extends FunSuite
    with SparkFactory
    with BeforeAndAfterAll {

  private var params: Map[String, String] = _
  private var customSchema: StructType = _

  override def beforeAll(): Unit = {
    params = Map(
      "host" -> "localhost",
      "port" -> "2222",
      "username" -> "foo",
      "password" -> "pass",
      "fileFormat" -> "csv",
      "path" -> "data/sparkdata",
      "delimiter" -> ",",
      "header" -> "true"
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

  test("Load dataset from remote host", IntegrationTest) {
    val rfr: RemoteFileReader = RemoteFileReader(
      spark.sqlContext,
      params ++ Map("path" -> "data/sparkdata/iris.csv"),
      null
    )
    val rdd: RDD[Row] = rfr.buildScan()
    assert(rdd.count() === 150)
  }

  test("Load dataset from remote host with custom schema", IntegrationTest) {
    val rfr: RemoteFileReader = RemoteFileReader(
      spark.sqlContext,
      params,
      customSchema
    )
    assert(rfr.schema() === customSchema)
    val rdd: RDD[Row] = rfr.buildScan()
    assert(rdd.count() === 300)
  }

}
