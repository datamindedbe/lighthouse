package be.dataminded.lighthouse.datalake

import be.dataminded.lighthouse.common.Database
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
  * Default JDBC DataRef implementation for reading and writing to a JDBC database
  *
  * @param url                Function returning the URL of the database you want to connect to. Should be in the following format
  *                           jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}
  * @param username           Function returning the Username of the database you want to connect to
  * @param password           Function returning the Password of the database you want to connect to
  * @param driver             Function returning the Driver to use for the database you want to connect to, should be available in
  *                           the classpath
  * @param table              Function returning the Table of the database where you would like to write to.
  * @param extraProperties    Additional properties to use to connect to the database
  * @param partitionColumn    The column where you want to partition your data for, should contain an Integer type
  * @param numberOfPartitions Amount of partitions you want to use for reading or writing your data. If value is 0 then
  *                           batchSize is taken to decide the number of partitions. If both numberOfPartitions and
  *                           batchSize is not 0, numberOfPartitions takes preference
  * @param batchSize          The amount of rows that you want to retrieve in one partition. If value is 0 number of partitions
  *                           is taken to decide the batch size
  * @param saveMode           Spark sql SaveMode
  */
class JdbcDataLink(url: LazyConfig[String],
                   username: LazyConfig[String],
                   password: LazyConfig[String],
                   driver: LazyConfig[String],
                   table: LazyConfig[String],
                   extraProperties: LazyConfig[Map[String, String]] = LazyConfig(Map.empty[String, String]),
                   partitionColumn: LazyConfig[String] = "",
                   numberOfPartitions: LazyConfig[Int] = 0,
                   batchSize: LazyConfig[Int] = 50000,
                   saveMode: SaveMode = SaveMode.Append)
    extends DataLink {

  // build the connection properties with some default extra ones
  lazy val connectionProperties: Map[String, String] = {
    Map(
      "url"      -> url(),
      "driver"   -> driver(),
      "dbtable"  -> table(),
      "user"     -> username(),
      "password" -> password()
    ) ++ extraProperties()
  }

  override def read(): DataFrame = {
    // Partition parameters are only applicable for read operation for now, order is important as some values can
    // be overwritten
    val props    = connectionProperties ++ partitionReadParams
    val sparkCtx = spark.read.format("jdbc").options(props)
    sparkCtx.load()
  }

  override def write[T](dataset: Dataset[T]): Unit = {
    dataset.write
      .mode(saveMode)
      .jdbc(connectionProperties("url"), connectionProperties("dbtable"), connectionProperties)
  }

  // Get the extra partition parameters
  private lazy val partitionReadParams: Map[String, String] = {
    val boundaries = getBoundaries(numberOfPartitions())

    (partitionColumn(), boundaries, numberOfPartitions(), batchSize()) match {
      // GUARD: If no partition column is provided.
      case (partition, _, _, _) if partition == null || partition.isEmpty => Map()

      // GUARD: If we were not able to retrieve the data boundaries.
      case (_, Failure(_), _, _) => Map()

      // When the number of partitions is set.
      case (partition, Success(Boundaries(min, max, _)), numPart, _) if numPart > 0 =>
        convertToReadParamsMap(partition, min, max, numPart)

      // When the batch size is set.
      case (partition, Success(Boundaries(min, max, count)), _, batch) if batch > 0 =>
        convertToReadParamsMap(partition, min, max, (count / batch) + 1)

      // In all other cases.
      case _ => Map()
    }
  }

  private case class Boundaries(min: Long, max: Long, count: Long)

  // The returns the minimum, maximum (and total) count of the partitionColumn
  private def getBoundaries(partitions: Int = numberOfPartitions()): Try[Boundaries] = {
    Try {
      if (partitionColumn().isEmpty) throw new IllegalStateException("No partition column defined")

      Database(driver(), connectionProperties("url"), connectionProperties).withConnection { connection =>
        val statement = connection.createStatement()

        // Only perform count when partitions are not set
        val query = partitions match {
          case 0 =>
            s"select min(${partitionColumn()}) as min, max(${partitionColumn()}) as max, count(${partitionColumn()}) as count from ${table()}"
          case p if p > 0 =>
            s"select min(${partitionColumn()}) as min, max(${partitionColumn()}) as max from ${table()}"
        }

        // Execute query, and parse based on whether we need to take count into account
        val result = (partitions, statement.execute(query), statement.getResultSet.next) match {
          case (p, true, true) if p == 0 =>
            Boundaries(
              statement.getResultSet.getBigDecimal("min").longValueExact(),
              statement.getResultSet.getBigDecimal("max").longValueExact(),
              statement.getResultSet.getBigDecimal("count").longValueExact()
            )
          case (p, true, true) if p > 0 =>
            Boundaries(
              statement.getResultSet.getBigDecimal("min").longValueExact(),
              statement.getResultSet.getBigDecimal("max").longValueExact(),
              0
            )
          case _ => throw new IllegalStateException("Min, max and count value could not be retrieved")
        }
        result
      }
    }
  }

  // Translate partition information to properties map usable by spark
  private def convertToReadParamsMap(partitionColumn: String,
                                     lowerBound: Long,
                                     upperBound: Long,
                                     numPartitions: Long): Map[String, String] = {
    Map("partitionColumn" -> partitionColumn,
        "lowerBound"      -> lowerBound.toString,
        "upperBound"      -> upperBound.toString,
        "numPartitions"   -> numPartitions.toString)
  }
}
