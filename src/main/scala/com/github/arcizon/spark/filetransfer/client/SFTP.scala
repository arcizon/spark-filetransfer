package com.github.arcizon.spark.filetransfer.client

import java.io.File
import java.nio.file.Paths

import com.github.arcizon.spark.filetransfer.util.FileTransferOptions
import com.jcraft.jsch.{ChannelSftp, JSch, JSchException, Session}
import org.apache.spark.internal.Logging

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
  def upload(src: String, dest: String): Unit = {
    val srcPath: File = new File(src)
    val files: List[File] = {
      Try(srcPath.listFiles().toList).getOrElse(List(srcPath))
    }

    val client: ChannelSftp = connect
    try {
      var path: String = ""
      for (dir <- dest.split(UNIX_PATH_SEPARATOR)) {
        path = path + UNIX_PATH_SEPARATOR + dir
        Try(client.mkdir(path))
      }

      files.foreach(x => {
        val source: String = x.getCanonicalPath
        val target: String = dest + UNIX_PATH_SEPARATOR + x.getName
        log.info("Uploading file from " + source + " to " + target)
        client.put(source, target)
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
}
