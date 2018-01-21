package be.dataminded.lighthouse.config

import java.time.LocalDate

import be.dataminded.lighthouse.datalake.Datalake
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class LighthouseConfigurationParserTest extends FunSuite with Matchers with BeforeAndAfterEach {

  val parser = new LighthouseConfigurationParser()

  test("Airflow default date can be parsed") {
    val config = parser.parse(List("--localdate", "2018-01-22", "-e", "test"), LighthouseConfiguration())

    config should equal(Some(LighthouseConfiguration(LocalDate.of(2018, 1, 22))))
  }

  test("Any date in the yyyy/MM/dd format can be parsed") {
    val config = parser.parse(List("--localdate", "2018/01/22", "-e", "test"), LighthouseConfiguration())

    config should equal(Some(LighthouseConfiguration(LocalDate.of(2018, 1, 22))))
  }

  test("The local date can also be passed using the shortcut '-d'") {
    val config = parser.parse(Seq("-d", "2018-01-22", "-e", "test"), LighthouseConfiguration())

    config should equal(Some(LighthouseConfiguration(LocalDate.of(2018, 1, 22))))
  }

  test("The local date is an optional parameter, by default it is going to pick the current date") {
    val config = parser.parse(Seq("-e", "test"), LighthouseConfiguration())

    config should equal(Some(LighthouseConfiguration(LocalDate.now())))
  }

  test("If the local date cannot be parsed the config is None") {
    val config = parser.parse(Seq("-d", "giberrish", "-e", "test"), LighthouseConfiguration())

    config should be(None)
  }

  test("When an environment is passed to the configuration, the environment is set as a system property") {
    val config = parser.parse(Seq("-e", "acc"), LighthouseConfiguration())

    System.getProperty(Datalake.SYSTEM_PROPERTY) should equal("acc")
    config should equal(Some(LighthouseConfiguration(environment = "acc")))
  }

  test("When environment is set as a system property, that value is being used") {
    System.setProperty(Datalake.SYSTEM_PROPERTY, "acc")

    val config = parser.parse(Seq.empty, LighthouseConfiguration())

    config should equal(Some(LighthouseConfiguration(environment = "acc")))
  }

  test("When no environment is set at all, the parsed config is None") {
    val config = parser.parse(Seq.empty, LighthouseConfiguration())

    config should be(None)
  }


  override protected def beforeEach(): Unit = {
    System.clearProperty(Datalake.SYSTEM_PROPERTY)
  }
}
