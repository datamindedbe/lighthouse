package be.dataminded.lighthouse.config

import java.time.LocalDate
import org.scalatest.{FunSuite, Matchers}

class LocalDateConfigurationParserTest extends FunSuite with Matchers {

  test("Airflow default date can be parsed") {
    val parser = new LocalDateConfigurationParser()
    val config = parser.parse(args = List("--localdate" ,"2018-01-22"), LocalDateConfiguration())

    config shouldBe Some(LocalDateConfiguration(LocalDate.of(2018, 1, 22)))
  }
}
