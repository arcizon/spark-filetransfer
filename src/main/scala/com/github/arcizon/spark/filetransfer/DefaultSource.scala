package com.github.arcizon.spark.filetransfer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.{
  BaseRelation,
  RelationProvider,
  SchemaRelationProvider,
  CreatableRelationProvider,
  DataSourceRegister
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Provides access to remote file data sources.
  *
  * @since 0.1.0
  */
class DefaultSource
    extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with Logging {

  /**
    * The string that represents the alias for the datasource.
    * The defined value is '''filetransfer'''.
    *
    * @since 0.1.0
    */
  override def shortName(): String = "filetransfer"

  override def toString: String = "FileTransfer"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[DefaultSource]

  /**
    * Returns a new base relation with the given parameters.
    *
    * @note The parameters' keywords are case insensitive and this insensitivity is enforced
    *       by the Map that is passed to the function.
    *
    * @since 0.1.0
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
    * Returns a new base relation with the given parameters and user defined schema.
    *
    * @note The parameters' keywords are case insensitive and this insensitivity
    *       is enforced by the Map that is passed to the function.
    *
    * @since 0.1.0
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType
  ): BaseRelation = {
    RemoteFileReader(sqlContext, parameters, schema)
  }

  /**
    * Saves a DataFrame to a destination (using data source-specific parameters)
    *
    * @param sqlContext SQLContext
    * @param mode specifies what happens when the destination already exists
    * @param parameters data source-specific parameters
    * @param data DataFrame to save (i.e. the rows after executing the query)
    * @return Relation with a known schema
    *
    * @since 0.1.0
    */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    RemoteFileWriter(sqlContext, mode, parameters, data)
  }
}
