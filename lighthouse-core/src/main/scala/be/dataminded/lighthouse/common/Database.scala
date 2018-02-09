package be.dataminded.lighthouse.common

import java.sql.{Connection, DriverManager}

import be.dataminded.lighthouse.datalake._

class Database(val driverClassName: String, url: String, properties: Map[String, String] = Map.empty) {

  def withConnection[A](autoCommit: Boolean)(block: (Connection) => A): A = {
    val connection = createConnection(autoCommit)
    try {
      block(connection)
    } finally {
      connection.close()
    }
  }

  def withConnection[A](block: (Connection) => A): A = withConnection(autoCommit = true)(block)

  private def createConnection(autoCommit: Boolean): Connection = {
    Class.forName(driverClassName)
    val connection = DriverManager.getConnection(url, properties)
    connection.setAutoCommit(autoCommit)
    connection
  }
}

object Database {

  def apply(driver: String, url: String, properties: Map[String, String] = Map.empty): Database =
    new Database(driver, url, properties)

  def inMemory(name: String, urlOptions: Map[String, String] = Map.empty): Database = {
    val urlExtra = urlOptions.map { case (k, v) => s"$k=$v" }.mkString(";", ";", "")
    val url      = s"jdbc:h2:mem:$name$urlExtra;"
    new Database("org.h2.Driver", url)
  }
}
