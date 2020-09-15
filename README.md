# Spark File Transfer Library

A library for reading and writing remote data files via various file transfer protocols like FTP, SFTP and SCP.

---
**NOTE**

As of current release v0.1.0, only SFTP support has been implemented.

---

![CI](https://github.com/arcizon/spark-filetransfer/workflows/ci/badge.svg)
[![License](https://img.shields.io/github/license/arcizon/spark-filetransfer)](LICENSE)
![GitHub last commit](https://img.shields.io/github/last-commit/arcizon/spark-filetransfer)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/arcizon/spark-filetransfer)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.arcizon/spark-filetransfer_2.12)](https://repo1.maven.org/maven2/com/github/arcizon/spark-filetransfer_2.12)
[![javadoc](https://javadoc.io/badge2/com.github.arcizon/spark-filetransfer_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.arcizon/spark-filetransfer_2.12)
[![codecov](https://codecov.io/gh/arcizon/spark-filetransfer/branch/main/graph/badge.svg?token=7FMIGEFTFO)](https://codecov.io/gh/arcizon/spark-filetransfer)

## Requirements

This library requires Apache Spark 2.x

## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.11
```
groupId: com.github.arcizon
artifactId: spark-filetransfer_2.11
version: 0.1.0
```
### Scala 2.12
```
groupId: com.github.arcizon
artifactId: spark-filetransfer_2.12
version: 0.1.0
```

## Using with Spark shell
This package can be added to Spark using the `--packages` command line option.
For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.github.arcizon:spark-filetransfer_2.11:0.1.0
```

### Spark compiled with Scala 2.12
```
$SPARK_HOME/bin/spark-shell --packages com.github.arcizon:spark-filetransfer_2.12:0.1.0
```

### Spark with Python 3
```
$SPARK_HOME/bin/pyspark --packages com.github.arcizon:spark-filetransfer_2.11:0.1.0

$SPARK_HOME/bin/pyspark --packages com.github.arcizon:spark-filetransfer_2.12:0.1.0
```

## Options for Spark DataFrame API
The following options are identified by this package to connect to the remote host
while accessing the files for reading and writing via Spark DataFrame API.

* `protocol` - File transfer protocol to be used for accessing remote files.
Allowed protocols are ftp, sftp, scp. Default set to __sftp__.
* `host` - Hostname of the remote machine to connect to. Default set to __localhost__.
* `port` - Port of the remote machine to connect to. Default set to __22__.
* `username` - Username for authentication to the remote machine access via protocol.
* `password` - Password for authentication to the remote machine access via protocol.
Optional if **keyFilePath** is provided.
* `keyFilePath` - Private key file path for authentication to the remote machine access via protocol.
Optional if **password** is provided.
* `passphrase` - Passphrase for the private key file supplied for authentication.
* `fileFormat` - File format of the remote file to read/write.
Allowed file formats are avro, csv, json, orc, parquet, text, xml. Non-native Spark datasources
like _avro_ [from Spark 2.4+](https://spark.apache.org/docs/2.4.0/sql-data-sources-avro.html#deploying)
and _xml_ [datasource](https://github.com/databricks/spark-xml#linking) expects their datasource packages
to be available on classpath to work.
* `localTempPath` - Temporary directory on the local disk. Default set to the value of Java System
Property **java.io.tmpdir** with a fallback default to **/tmp**. For every run, a uniquely named subdirectory
(marked for deletion upon JVM exit) gets created within this directory for storing the files that are being
uploaded/downloaded from remote machine via provided protocol.
* `dfsTempPath` - Temporary directory on the distributed filesystem. Default set to the value
of **localTempPath** option. For every run, a uniquely named subdirectory
(marked for deletion upon JVM exit) gets created within this directory for copying the locally downloaded
files and also for writing the DataFrame output of the upload content.
__To achieve parallelism, you need to set this value to a distributed filesystem path that is accessible
by your SparkSession.__
* `uploadFilePrefix` - Prefix to be used for the file to be uploaded. Default set to __part__.
All the files to be uploaded during write operation will be of a consistent format
`<uploadFilePrefix>-<upload_files_count_index>.<fileformat>`, example: part-0.csv, part-1.csv and so on.
* `path` - Location of the file/directory on the remote machine accessible via protocol.
Can be skipped if provided as parameter to Spark DataFrame API's load() or save() method.

---

Any additional options supplied to the DataFrame API other than above are forwarded to the
provided `fileFormat` datasource API. This may include options like delimiter, header, etc., for *csv*,
and multiline for *json* and so on.

---

## Usage
This package is registered as a datasource against Spark Datasource Register which allows you to
use short name `filetransfer` instead of the full package name `com.github.arcizon.spark.filetransfer`
for the __format__.

### Scala API
```scala
// Construct Spark DataFrame from CSV files directory on the remote machine via provided protocol
val df = spark.read
  .format("filetransfer")
  .option("protocol", "sftp")
  .option("host", "example.com")
  .option("port", "22")
  .option("username", "foo")
  .option("password", "pass")
  .option("fileFormat", "csv")
  .option("delimiter", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("dfsTempPath", "hdfs:///test/tmp")
  .load("data/sparkdata/")

// Write Spark DataFrame in JSON File format on the remote machine via provided protocol
df.write
  .format("filetransfer")
  .option("protocol", "sftp")
  .option("host", "example.com")
  .option("port", "22")
  .option("username", "foo")
  .option("password", "pass")
  .option("fileFormat", "json")
  .option("uploadFilePrefix", "sample")
  .option("dfsTempPath", "hdfs:///test/tmp")
  .save("data/upload/output/")
```

### Java API
```java
// Construct Spark DataFrame from TEXT file on the remote machine via provided protocol
Dataset<Row> df = spark.read()
    .format("filetransfer")
    .option("protocol", "sftp")
    .option("host", "example.com")
    .option("port", "22")
    .option("username", "foo")
    .option("password", "pass")
    .option("fileFormat", "text")
    .option("dfsTempPath", "hdfs:///test/tmp")
    .load("data/example.txt");

// Write Spark DataFrame in AVRO File format on the remote machine via provided protocol
df.write()
    .format("filetransfer")
    .option("protocol", "sftp")
    .option("host", "example.com")
    .option("port", "22")
    .option("username", "foo")
    .option("password", "pass")
    .option("fileFormat", "avro")
    .option("dfsTempPath", "hdfs:///test/tmp")
    .save("data/upload/out/");
```

### Python API
```python
## Construct Spark DataFrame from JSON file on the remote machine via provided protocol
df = spark.read \
    .format("filetransfer") \
    .option("protocol", "sftp") \
    .option("host", "example.com") \
    .option("port", "22") \
    .option("username", "foo") \
    .option("password", "pass") \
    .option("fileFormat", "json") \
    .option("dfsTempPath", "hdfs:///test/tmp") \
    .load("data/sparkdata/sample.json")

## Write Spark DataFrame in ORC File format on the remote machine via provided protocol
df.write \
    .format("filetransfer") \
    .option("protocol", "sftp") \
    .option("host", "example.com") \
    .option("port", "22") \
    .option("username", "foo") \
    .option("password", "pass") \
    .option("fileFormat", "orc") \
    .option("dfsTempPath", "hdfs:///test/tmp") \
    .save("data/upload/output/")
```

### R API
```r
library(SparkR)

sparkR.session("local[4]", sparkPackages = c("com.github.arcizon:spark-filetransfer_2.12:0.1.0"))

## Construct Spark DataFrame from CSV files directory on the remote machine via provided protocol
df <- read.df(path="data/sparkdata/iris.csv",
            source="filetransfer",
            host="example.com",
            port="22",
            username="foo",
            password="pass",
            fileFormat="csv",
            delimiter=",",
            header="true")

## Write Spark DataFrame in PARQUET File format on the remote machine via provided protocol
write.df(df,
        path="data/upload/output/",
        source="filetransfer",
        host="example.com",
        port="22",
        username="foo",
        password="pass",
        fileFormat="parquet")
```

## Building From Source
The build configuration includes support for both Scala 2.11 and 2.12.<br/>
To build a JAR file for latest Scala 2.12 simply run `./gradlew build` from the project root.<br/>
To build JARs for both Scala 2.11 and 2.12 run `./gradlew build -PallScalaVersions`.<br/>
