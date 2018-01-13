package be.dataminded.lighthouse.common

import java.time.format.DateTimeFormatter

object DateTimeFormatters {

  val SimpleDateFormat: DateTimeFormatter      = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  val PartitionedDateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("'year='yyyy/'month='MM/'day='dd")

}
