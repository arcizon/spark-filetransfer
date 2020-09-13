package com.github.arcizon.spark.filetransfer.testfactory

import java.io.File

import com.github.arcizon.spark.filetransfer.util.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfterEach, Suite}

trait SparkFactory extends BeforeAndAfterEach with Logging {
  this: Suite =>

  var appLogLevel: Level = Level.INFO
  var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = getSparkSession()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.stop()
  }

  def getSparkSession(
      conf: Option[SparkConf] = None,
      enableHiveSupport: Boolean = true
  ): SparkSession = {
    val ssb: SparkSession.Builder =
      SparkSession.builder.config(sparkConfig(conf))
    if (enableHiveSupport) ssb.enableHiveSupport()
    val ss: SparkSession = ssb.getOrCreate()
    ss.sparkContext.setLogLevel(appLogLevel.toString)
    ss
  }

  def sparkConfig(sparkConf: Option[SparkConf]): SparkConf = {
    if (sparkConf.isEmpty) {
      val tempDir: File = FileUtils.createTempDir(
        System.getProperty("java.io.tmpdir", "/tmp")
      )
      val localWarehousePath: File = new File(tempDir, "warehouse")
      val localMetastorePath: File = new File(tempDir, "metastore")

      val conf: SparkConf = new SparkConf()
      conf.setAppName("SparkFileTransferTesting")
      conf.set("spark.app.id", applicationID)
      conf.setMaster("local[2]")
      conf.set("spark.ui.enabled", "false")
      conf.set(SQLConf.CODEGEN_FALLBACK.key, "false")
      conf.set(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${localMetastorePath.getCanonicalPath};create=true"
      )
      conf.set(
        "datanucleus.rdbms.datastoreAdapterClassName",
        "org.datanucleus.store.rdbms.adapter.DerbyAdapter"
      )
      conf.set(ConfVars.METASTOREURIS.varname, "")
      conf.set(
        "spark.sql.streaming.checkpointLocation",
        tempDir.toPath.toString
      )
      conf.set("spark.sql.warehouse.dir", localWarehousePath.getCanonicalPath)
      conf.set("spark.sql.shuffle.partitions", "1")
      conf.set("spark.launcher.childProcLoggerName", "sftlogger")
      conf.set("spark.sql.parquet.compression.codec", "snappy")
      conf.set("hive.exec.dynamic.partition", "true")
      conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      conf
    } else {
      sparkConf.get
    }
  }

  def applicationID: String =
    this.getClass.getName + math.floor(math.random * 10e4).toLong.toString

  private case class WrappedConfVar(cv: ConfVars) {
    val varname: String = cv.varname

    def getDefaultExpr: String = cv.getDefaultExpr
  }

}
