package com.github.arcizon.spark.filetransfer.client

import java.io.File
import java.nio.file.Paths

import com.github.arcizon.spark.filetransfer.util.{
  FileTransferOptions,
  FileUtils
}
import com.jcraft.jsch.{
  ChannelSftp,
  JSch,
  JSchException,
  Session,
  SftpException
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * The `SFTPClient` class provides `upload` and `download` methods
  * to transfer files to/from remote host via '''SFTP'''.
  *
  * @constructor Create a new `SFTPClient` by specifying their `options`.
  * @param options Options passed by the user on the
  *                '''DataFrameReader''' or '''DataFrameWriter''' API.
  *
  * @since 0.1.0
  */
class SFTP(options: FileTransferOptions) extends BaseClient with Logging {

  private val STRICT_HOST_KEY_CHECKING: String = System.getProperty(
    "STRICT_HOST_KEY_CHECKING",
    "no"
  )
  private val UNIX_PATH_SEPARATOR: String = "/"

  @throws[JSchException]
  private def connect: ChannelSftp = {
    val jsch: JSch = new JSch()
    if (options.keyFilePath.isDefined) {
      jsch.addIdentity(options.keyFilePath.get, options.passphrase.orNull)
    }

    val session: Session = jsch.getSession(
      options.username,
      options.host,
      options.port
    )
    session.setConfig("StrictHostKeyChecking", STRICT_HOST_KEY_CHECKING)
    if (options.password.isDefined) {
      session.setPassword(options.password.get)
    }
    session.connect()

    val channel: ChannelSftp =
      session.openChannel("sftp").asInstanceOf[ChannelSftp]
    channel.connect()

    channel
  }

  private def disconnect(channel: ChannelSftp): Unit = {
    val session: Session = channel.getSession
    channel.exit()
    session.disconnect()
  }

  /** @inheritdoc */
  def upload(
      src: String,
      dest: String,
      mode: SaveMode = SaveMode.Overwrite
  ): Unit = {
    val srcPath: File = new File(src)
    val files: List[File] = {
      Try(srcPath.listFiles().toList).getOrElse(List(srcPath))
    }
    val filesSize = files.size
    val destExt: String = {
      Try(FileUtils.getFileExt(new File(dest))).getOrElse("")
    }
    if (destExt.nonEmpty && filesSize > 1) {
      sys.error(
        s"""
           |Too many source files '$filesSize' found to write to target file path '$dest'!!
           |Please provide directory path on remote machine or combine the source files to
           |a single file.
          """.stripMargin.trim
      )
    }
    if (destExt.nonEmpty && filesSize == 1) {
      val srcExt: String = FileUtils.getFileExt(files.head)
      if (srcExt != destExt)
        sys.error(
          s"Source '$srcExt' and Target '$destExt' file extensions mismatch!!"
        )
    }

    val client: ChannelSftp = connect
    try {
      var path: String = ""
      var destDirs: List[String] = dest.split(UNIX_PATH_SEPARATOR).toList
      if (destExt.nonEmpty) destDirs = destDirs.dropRight(1)
      for (dir <- destDirs) {
        path = path + UNIX_PATH_SEPARATOR + dir
        Try(client.mkdir(path))
      }

      files.foreach(x => {
        val source: String = x.getCanonicalPath
        val target: String = if (filesSize == 1 && destExt.nonEmpty) {
          dest
        } else {
          dest + UNIX_PATH_SEPARATOR + x.getName
        }
        log.info("Uploading file from " + source + " to " + target)
        val uploadMode: Int = getPutMode(client, target, mode)
        if (uploadMode != -1) {
          client.put(source, target, uploadMode)
        }
      })

    } finally {
      disconnect(client)
    }
  }

  /** @inheritdoc */
  def download(src: String, dest: String): Unit = {
    val client: ChannelSftp = connect
    var files: List[String] = List(src)

    try {
      if (client.stat(src).isDir) {
        files = client
          .ls(src)
          .asInstanceOf[java.util.Vector[client.LsEntry]]
          .asScala
          .filterNot(x => {
            Set(".", "..").contains(x.getFilename) || x.getAttrs.isDir
          })
          .map(x => src + UNIX_PATH_SEPARATOR + x.getFilename)
          .toList
      }

      for (source <- files) {
        val target: String = dest + File.separator + Paths
          .get(source)
          .getFileName
        log.info("Downloading file from " + source + " to " + target)
        client.get(source, target)
      }
    } finally {
      disconnect(client)
    }
  }

  /**
    * Gets the SFTP upload mode based on the provided Spark DataFrameWriter save mode.
    *
    * @param client SFTP client instance.
    * @param target Target file path to upload on the remote machine.
    * @param mode Spark DataFrame save mode to determine the file upload mode.
    * @return SFTP upload mode.
    *
    * @since 0.3.0
    */
  def getPutMode(
      client: ChannelSftp,
      target: String,
      mode: SaveMode
  ): Int = mode match {
    case x @ (SaveMode.ErrorIfExists | SaveMode.Ignore) =>
      var fileExists: Boolean = false
      try {
        fileExists = !client.ls(target).isEmpty
      } catch {
        case ex: SftpException =>
          if (ex.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) fileExists = false
      }
      if (fileExists) {
        if (x == SaveMode.ErrorIfExists) {
          sys.error(
            s"Target file '$target' already exists on remote host '${options.host}'!!"
          )
        } else {
          log.info(
            s"Ignoring target file '$target' write as it already exists on remote host '${options.host}'!!"
          )
          -1
        }
      } else {
        ChannelSftp.OVERWRITE
      }
    case SaveMode.Append    => ChannelSftp.APPEND
    case SaveMode.Overwrite => ChannelSftp.OVERWRITE
  }
}
