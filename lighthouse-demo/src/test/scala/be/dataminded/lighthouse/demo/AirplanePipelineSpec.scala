package be.dataminded.lighthouse.demo

import be.dataminded.lighthouse.testing.SparkFunSuite
import better.files._
import org.scalatest.Matchers


class AirplanePipelineSpec extends SparkFunSuite with AirplanePipeline with Matchers {

  sparkTest("Run the pipeline") {
    pipeline.run(spark)

    file"target/clean/airplane".glob("*.orc").length should be (1)
    file"target/clean/weather/daily".glob("*.orc").length should be (1)
    file"target/clean/weather/stations".glob("*.orc").length should be (1)
    file"target/master/airplane/view".glob("*.orc").length should not be 0
  }
}
