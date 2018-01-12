package be.dataminded.lighthouse.common

import java.time.format.DateTimeFormatter

object DateTimeFormatters {

  val SIMPLE_DATE_FORMAT: DateTimeFormatter      = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  val PARTITIONED_DATE_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("year=yyyy/month=MM/day=dd")

}
