package com.github.arcizon.spark.filetransfer.client

import java.io.File
import java.util.UUID

import com.github.arcizon.spark.filetransfer.testfactory.{
  FileFactory,
  IntegrationTest
}
import com.github.arcizon.spark.filetransfer.util.FileTransferOptions
import org.apache.spark.sql.SaveMode
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

class SFTPTest extends FunSuite with FileFactory with MockFactory {

  private var params: Map[String, String] = _

  override def beforeEach(): Unit = {
    params = Map(
      "host" -> "localhost",
      "port" -> "2222",
      "username" -> "foo",
      "password" -> "pass",
      "fileFormat" -> "text"
    )
    file = new File(tmpDir, "download")
    file.mkdirs()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    params = Map.empty
  }

  test("Download file") {
    params += ("path" -> "data/error.txt")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    class MockableSFTP extends SFTP(fto)
    val mockSFTP: SFTP = stub[MockableSFTP]
    (mockSFTP.download _).when(params("path"), "testing.txt").returns(())
  }

  test("SFTP download unavailable file error", IntegrationTest) {
    params += ("path" -> "data/error.txt")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    assertThrows[Exception](sftp.download(params("path"), file.toString))
  }

  test("SFTP download file", IntegrationTest) {
    params += ("path" -> "data/download.txt")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    val downloadTo: File = new File(file, "download.txt")
    sftp.download(params("path"), file.toString)
    assert(downloadTo.exists())
  }

  test("Upload file") {
    params += ("path" -> "data/error.txt")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    class MockableSFTP extends SFTP(fto)
    val mockSFTP: SFTP = stub[MockableSFTP]
    (mockSFTP.upload _)
      .when("testing.txt", params("path"), SaveMode.Overwrite)
      .returns(())
  }

  test("SFTP upload files", IntegrationTest) {
    params += ("path" -> s"data/upload/multiple")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    val uploadFile: String =
      this.getClass.getResource("/sftp/sparkdata").getPath
    assertResult(())(
      sftp.upload(uploadFile, params("path"))
    )
  }

  test("SFTP single file upload already exists", IntegrationTest) {
    params += ("path" -> "data/upload/README.txt")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    val uploadFile: String =
      this.getClass.getResource("/sftp/upload/README.txt").getPath

    val caughtExists = intercept[RuntimeException](
      sftp.upload(uploadFile, params("path"), SaveMode.ErrorIfExists)
    )
    assert(caughtExists.getMessage.contains("already exists"))
  }

  test("SFTP single file upload ignore if already exists", IntegrationTest) {
    params += ("path" -> "data/upload/README.txt")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    val uploadFile: String =
      this.getClass.getResource("/sftp/upload/README.txt").getPath

    assertResult(())(
      sftp.upload(uploadFile, params("path"), SaveMode.Ignore)
    )
  }

  test("SFTP single file upload extensions mismatch", IntegrationTest) {
    params += ("path" -> "data/upload/README.csv")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    val uploadFile: String =
      this.getClass.getResource("/sftp/upload/README.txt").getPath

    val caughtExtMismatch = intercept[RuntimeException](
      sftp.upload(
        uploadFile,
        params("path"),
        SaveMode.ErrorIfExists
      )
    )
    assert(caughtExtMismatch.getMessage.contains("file extensions mismatch"))
  }

  test("SFTP single file upload to directory", IntegrationTest) {
    params += ("path" -> s"data/upload/${UUID.randomUUID().toString}")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    val uploadFile: String =
      this.getClass.getResource("/sftp/upload/README.txt").getPath

    assertResult(())(
      sftp.upload(uploadFile, params("path"), SaveMode.ErrorIfExists)
    )
  }

  test("SFTP single file upload has too many source files", IntegrationTest) {
    params += ("path" -> "data/upload/test.csv")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    val uploadFile: String =
      this.getClass.getResource("/sftp/sparkdata").getPath

    val caughtTooManySrcFiles = intercept[RuntimeException](
      sftp.upload(
        uploadFile,
        params("path"),
        SaveMode.ErrorIfExists
      )
    )
    assert(caughtTooManySrcFiles.getMessage.startsWith("Too many source files"))
  }

  test("SFTP upload unavailable file error", IntegrationTest) {
    val uploadFile: String = this.getClass.getResource("/upload.txt").getPath
    params += ("path" -> uploadFile)
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    assertThrows[Exception](
      sftp.upload("/error.txt", params("path"))
    )
  }
}
