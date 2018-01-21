package be.dataminded.lighthouse

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import be.dataminded.lighthouse.common.DateTimeFormatters
import scopt.Read
import scopt.Read.reads

import scala.util.{Failure, Success, Try}

package object config {

  implicit val LocalDateSupport: Read[LocalDate] = reads { timestamp =>
    tryParseLocalDate(timestamp) match {
      case Success(localDate) => localDate
      case Failure(e)         => throw new IllegalArgumentException(s"The given timestamp: [$timestamp] could not be parsed", e)
    }
  }

  private def tryParseLocalDate(timestamp: String): Try[LocalDate] = {
    Try {
      LocalDate.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE)
    } recover {
      case _ =>
        LocalDate.parse(timestamp, DateTimeFormatters.SimpleDateFormat)
    }
  }
}
