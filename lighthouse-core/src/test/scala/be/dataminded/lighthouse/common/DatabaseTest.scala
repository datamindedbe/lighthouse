package be.dataminded.lighthouse.common

import org.scalatest.{FunSpec, Matchers}

class DatabaseTest extends FunSpec with Matchers {

  describe("An in-memory database") {

    val database = Database.inMemory("test")

    it("can easily be created using the factory method") {
      database.driverClassName should equal("org.h2.Driver")
    }

    it("has a function for using the connection") {
      database.withConnection { connection =>
        connection.createStatement.execute("CREATE TABLE nothing (id varchar, name varchar)")
      }
    }

    it("is by default auto-commited") {
      val isAutoCommit = database.withConnection { connection =>
        connection.getAutoCommit
      }
      isAutoCommit should be(true)
    }

    it("can be configured to not be auto-commited") {
      val isAutoCommit = database.withConnection(autoCommit = false) { connection =>
        connection.getAutoCommit
      }
      isAutoCommit should be(false)
    }
  }
}
