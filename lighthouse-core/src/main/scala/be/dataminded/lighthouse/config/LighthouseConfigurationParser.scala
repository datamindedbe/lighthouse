package be.dataminded.lighthouse.config

import java.time.LocalDate

import be.dataminded.lighthouse.datalake.Datalake
import org.apache.spark.SparkConf
import scopt.OptionParser

/**
  * Default configuration parser for projects that use Lighthouse.
  *
  * Requires the environment to be set through either:
  *
  * - An application parameter (-e, --environment)
  * - A system property (-Dlighthouse.environment)
  * - An environment variable (export LIGHTHOUSE_ENVIRONMENT=xxx)
  * - in the spark-submit as an extra option (spark-submit --conf lighthouse.environment=xxx)
  * - configured in the spark-defaults.conf as (lighthouse.environment = xxx)
  *
  * An optional local date can also be passed.
  */
class LighthouseConfigurationParser extends OptionParser[LighthouseConfiguration]("lighthouse") {
  override def showUsageOnError: Boolean = true

  opt[LocalDate]('d', "localdate")
    .action((localDate, config) => config.copy(localDate = localDate))
    .text("The localdate for which the job has to run")
    .optional()

  opt[String]('e', "environment")
    .action((environment, config) => config.copy(environment = environment))
    .withFallback((() => LighthouseConfigurationParser.this.fallbackEnvironment()))
    .validate(item => if (item.nonEmpty) success else failure("The configured environment for Lighthouse is empty"))
    .required()

  override def parse(args: Seq[String], init: LighthouseConfiguration): Option[LighthouseConfiguration] = {
    super.parse(args, init) match {
      case success @ Some(config) =>
        System.setProperty(Datalake.PropertyName, config.environment)
        success
      case None => None
    }
  }

  private def fallbackEnvironment(): String = {
    Option(System.getProperty(Datalake.PropertyName))
      .orElse(Option(System.getenv("LIGHTHOUSE_ENVIRONMENT")))
      .getOrElse(new SparkConf().get(Datalake.PropertyName, ""))

  }
}

case class LighthouseConfiguration(localDate: LocalDate = LocalDate.now(), environment: String = "test")
