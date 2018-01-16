package be.dataminded.lighthouse.config

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ofPattern

import be.dataminded.lighthouse.common.DateTimeFormatters
import scopt.OptionParser

import scala.util.Try

/**
  * Default configuration parser taking into account only the local date
  */
class LocalDateConfigurationParser extends OptionParser[LocalDateConfiguration]("lighthouse") {
  override def showUsageOnError: Boolean = true

  opt[String]("localdate")
    .action((localDate, config) => config.copy(localDate = tryParseTimestamp(localDate)))
    .text("The localdate for which the job has to run")
    .optional()

  private def tryParseTimestamp(timestamp: String): LocalDate = {
    (Try {
      LocalDate.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE)
    } recover {
      case _ =>
        LocalDate.parse(timestamp, DateTimeFormatters.SimpleDateFormat)
    }).getOrElse(throw new IllegalArgumentException(s"The given timestamp: [$timestamp] could not be parsed"))
  }
}

case class LocalDateConfiguration(localDate: LocalDate = LocalDate.now())
