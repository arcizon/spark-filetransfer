package com.github.arcizon.spark.filetransfer

import java.util.UUID

import com.github.arcizon.spark.filetransfer.testfactory.{
  IntegrationTest,
  SparkFactory
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.FunSuite

class DefaultSourceTest extends FunSuite with SparkFactory {

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
      "path" -> "data/sparkdata/iris.csv",
      "delimiter" -> ",",
      "header" -> "true"
    )
    val df: DataFrame = spark.read
      .format("filetransfer")
      .options(readOptions)
      .load()
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
      "path" -> s"data/upload/${UUID.randomUUID().toString}",
      "multiLine" -> "true"
    )
    df.write
      .format(this.getClass.getPackage.getName)
      .mode(SaveMode.Overwrite)
      .options(writeOptions)
      .save()
  }

}
