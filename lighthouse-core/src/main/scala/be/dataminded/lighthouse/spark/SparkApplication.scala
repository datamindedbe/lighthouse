package be.dataminded.lighthouse.spark

/**
  * Base class for writing a Spark-based application. When implementing this interface you have to name you're application.
  *
  * An instance of [[org.apache.spark.sql.SparkSession]] will be provided for you.
  */
trait SparkApplication extends App with SparkSessions
