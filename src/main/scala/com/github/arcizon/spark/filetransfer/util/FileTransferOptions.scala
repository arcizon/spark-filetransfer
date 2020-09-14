package com.github.arcizon.spark.filetransfer.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
  * @constructor Create a new `FileTransferOptions` by specifying their `parameters`.
  *
  * @param parameters Options specified on the '''DataFrame API''' Reader/Writer.
  *
  * @since 0.1.0
  */
class FileTransferOptions(
    @transient private val parameters: CaseInsensitiveMap[String]
) extends Serializable
    with Logging {

  import FileTransferOptions._

  /**
    * @constructor Create a new `FileTransferOptions` by specifying their `parameters`.
    *
    * @param parameters Options specified on the '''DataFrame API''' Reader/Writer.
    *
    * @since 0.1.0
    */
  def this(parameters: Map[String, String]) =
    this(CaseInsensitiveMap(parameters))

  /**
    * Value set with the option key __protocol__.
    * Default value set to ''sftp''.
    *
    * @since 0.1.0
    */
  val protocol: Protocol.Value = Protocol.withName(
    parameters.getOrElse(PROTOCOL, Protocol.sftp.toString).toLowerCase
  )

  /**
    * Value set with the option key __username__.
    *
    * @throws java.lang.RuntimeException Missing 'username' option.
    *
    * @since 0.1.0
    */
  val username: String = parameters.getOrElse(
    USERNAME,
    sys.error(s"Missing '$USERNAME' option!!")
  )

  /**
    * Value set with the option key __password__.
    *
    * @since 0.1.0
    */
  val password: Option[String] = parameters.get(PASSWORD)

  /**
    * Value set with the option key __keyFilePath__.
    *
    * @since 0.1.0
    */
  val keyFilePath: Option[String] = parameters.get(KEY_FILE_PATH)

  require(
    keyFilePath.isDefined != password.isDefined,
    s"Either '$KEY_FILE_PATH' or '$PASSWORD' is required to establish remote connection!!"
  )

  /**
    * Value set with the option key __passphrase__.
    *
    * @since 0.1.0
    */
  val passphrase: Option[String] = parameters.get(PASSPHRASE)

  /**
    * Value set with the option key __host__.
    * Default value set to ''localhost''.
    *
    * @since 0.1.0
    */
  val host: String = parameters.getOrElse(HOST, "localhost")

  /**
    * Value set with the option key __port__.
    * Default value set to ''22''.
    *
    * @since 0.1.0
    */
  val port: Int = parameters.getOrElse(PORT, "22").toInt

  /**
    * Value set with the option key __fileFormat__.
    *
    * @throws java.lang.RuntimeException Missing 'fileFormat' option.
    *
    * @since 0.1.0
    */
  val fileFormat: FileFormat.Value = FileFormat.withName(
    parameters
      .getOrElse(
        FILE_FORMAT,
        sys.error(
          s"Missing '$FILE_FORMAT' option with one of ${FileFormat.values.toList} value!!"
        )
      )
      .toLowerCase
  )

  /**
    * Value set with the option key __path__.
    *
    * @throws java.lang.RuntimeException Missing 'path' option.
    *
    * @since 0.1.0
    */
  val path: String =
    parameters.getOrElse(PATH, sys.error(s"Missing '$PATH' option!!"))

  /**
    * Value set with the option key __localTempPath__.
    * Default value set to java system property of `java.io.tmpdir`
    * with a fallback value of `/tmp`.
    *
    * @since 0.1.0
    */
  val localTempPath: String = parameters.getOrElse(
    LOCAL_TEMP_PATH,
    System.getProperty("java.io.tmpdir", "/tmp")
  )

  /**
    * Value set with the option key __dfsTempPath__.
    * Default value set to option value from `localTempPath`.
    *
    * @note If '''dfsTempPath''' is not provided then the downloaded
    *       file from remote host would be on the local path which
    *       is not splittable and therefore, parallelism might not
    *       be achieved during the file scan to load data.
    *
    * @since 0.1.0
    */
  val dfsTempPath: String = parameters.getOrElse(DFS_TEMP_PATH, localTempPath)

  /**
    * Value set with the option key __uploadFilePrefix__.
    * Default value set to `part`.
    *
    * @since 0.1.0
    */
  val uploadFilePrefix: String = parameters.getOrElse(
    UPLOAD_FILE_PREFIX,
    "part"
  )

  /**
    * All other options except the extracted options from the above
    * to be passed along to the '''DataFrame API''' for read/write.
    * For example, csv options like ''delimiter'' for the `csv` file format etc.
    *
    * @since 0.1.0
    */
  val dfOptions: Map[String, String] = parameters -- excludeOptionsToDataFrame
}

/**
  * Constants for the spark file transfer option keys
  * passed with the '''DataFrame API'''.
  *
  * @since 0.1.0
  */
object FileTransferOptions {

  /**
    * Constant for the option key __protocol__.
    *
    * @since 0.1.0
    */
  val PROTOCOL: String = "protocol"

  /**
    * Constant for the option key __username__.
    *
    * @since 0.1.0
    */
  val USERNAME: String = "username"

  /**
    * Constant for the option key __password__.
    *
    * @since 0.1.0
    */
  val PASSWORD: String = "password"

  /**
    * Constant for the option key __keyFilePath__.
    *
    * @since 0.1.0
    */
  val KEY_FILE_PATH: String = "keyFilePath"

  /**
    * Constant for the option key __passphrase__.
    *
    * @since 0.1.0
    */
  val PASSPHRASE: String = "passphrase"

  /**
    * Constant for the option key __host__.
    *
    * @since 0.1.0
    */
  val HOST: String = "host"

  /**
    * Constant for the option key __port__.
    *
    * @since 0.1.0
    */
  val PORT: String = "port"

  /**
    * Constant for the option key __fileFormat__.
    *
    * @since 0.1.0
    */
  val FILE_FORMAT: String = "fileFormat"

  /**
    * Constant for the option key __path__.
    *
    * @since 0.1.0
    */
  val PATH: String = "path"

  /**
    * Constant for the option key __localTempPath__.
    *
    * @since 0.1.0
    */
  val LOCAL_TEMP_PATH: String = "localTempPath"

  /**
    * Constant for the option key __dfsTempPath__.
    *
    * @since 0.1.0
    */
  val DFS_TEMP_PATH: String = "dfsTempPath"

  /**
    * Constant for the option key __uploadFilePrefix__.
    *
    * @since 0.1.0
    */
  val UPLOAD_FILE_PREFIX: String = "uploadFilePrefix"

  /**
    * Set of all the Spark File Transfer options above
    * that will be excluded from the overall options
    * set on the DataFrame API.
    *
    * @since 0.1.0
    */
  val excludeOptionsToDataFrame: Set[String] = Set(
    PROTOCOL,
    USERNAME,
    PASSWORD,
    KEY_FILE_PATH,
    PASSPHRASE,
    HOST,
    PORT,
    FILE_FORMAT,
    PATH,
    LOCAL_TEMP_PATH,
    DFS_TEMP_PATH,
    UPLOAD_FILE_PREFIX
  )
}
