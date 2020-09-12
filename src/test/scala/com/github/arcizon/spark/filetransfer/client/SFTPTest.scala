package com.github.arcizon.spark.filetransfer.client

import java.io.File
import java.util.UUID

import com.github.arcizon.spark.filetransfer.testfactory.{
  FileFactory,
  IntegrationTest
}
import com.github.arcizon.spark.filetransfer.util.FileTransferOptions
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
    (mockSFTP.upload _).when("testing.txt", params("path")).returns(())
  }

  test("SFTP upload file", IntegrationTest) {
    val uploadedFile: String = UUID.randomUUID().toString + ".txt"
    params += ("path" -> s"data/upload/$uploadedFile")
    val fto: FileTransferOptions = new FileTransferOptions(params)
    val sftp: SFTP = new SFTP(fto)
    val uploadFile: String = this.getClass.getResource("/upload.txt").getPath
    assertResult(())(
      sftp.upload(uploadFile, params("path"))
    )
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
