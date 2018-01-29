package be.dataminded.lighthouse.datalake

import java.util.Properties

import org.scalatest.{FunSpec, Matchers}

class packageTest extends FunSpec with Matchers {

  describe("asProperties") {
    it("should convert a Scala to Java Properties implicitly") {
      val properties: Properties = Map("test1" -> "1", "test2" -> "2")

      properties.getProperty("test1") should equal("1")
      properties.getProperty("test2") should equal("2")
    }

    it("does only convert maps of type Map[String, String]") {
      assertDoesNotCompile("val properties: Properties = Map(\"test1\" -> 1, \"test2\" -> 2)")
    }
  }
}
