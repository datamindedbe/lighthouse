package be.dataminded.lighthouse.datalake

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

/**
  * Default JDBC DataRef implementation for reading and writing to a JDBC database
  *
  * @param url Function returning the URL of the database you want to connect to. Should be in the following format
  *            jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}
  * @param username Function returning the Username of the database you want to connect to
  * @param password Function returning the Password of the database you want to connect to
  * @param driver Function returning the Driver to use for the database you want to connect to, should be available in
  *               the classpath
  * @param table Function returning the Table of the database where you would like to write to.
  */
class JdbcDataLink(url: LazyConfig[String],
                   username: LazyConfig[String],
                   password: LazyConfig[String],
                   driver: LazyConfig[String],
                   table: LazyConfig[String],
                   extraProperties: Map[String, String] = Map.empty,
                   saveMode: SaveMode = SaveMode.Append)
    extends DataLink {

  // build the connection properties with some default extra ones
  lazy val connectionProperties: Map[String, String] = {
    Map(
      "url"                      -> url(),
      "driver"                   -> driver(),
      "table"                    -> table(),
      "user"                     -> username(),
      "password"                 -> password(),
      "autoReconnect"            -> "true",
      "failOverReadOnly"         -> "false",
      "rewriteBatchedStatements" -> "true",
      "useSSL"                   -> "false",
      "zeroDateTimeBehavior"     -> "convertToNull",
      "transformedBitIsBoolean"  -> "true"
    ) ++ extraProperties
  }

  override def read(): DataFrame = {
    spark.read.jdbc(connectionProperties("url"), connectionProperties("table"), connectionProperties)
  }

  override def write[T](dataset: Dataset[T]): Unit = {
    dataset.write.mode(saveMode).jdbc(connectionProperties("url"), connectionProperties("table"), connectionProperties)
  }
}
