package be.dataminded.lighthouse.common

import java.time.LocalDate

import be.dataminded.lighthouse.common.DateTimeFormatters._
import org.scalatest.{FunSuite, Matchers}

class DateTimeFormattersTest extends FunSuite with Matchers {

  test("Simple Date Format should format a date like yyyy/MM/dd") {
    val result = SimpleDateFormat.format(LocalDate.of(1990, 1, 1))

    result should equal ("1990/01/01")
  }

  test("Partitioned Date Format should format a date like year=yyyy/month=MM/day=dd") {
    val result = PartitionedDateFormat.format(LocalDate.of(1990, 1,1))

    result should equal ("year=1990/month=01/day=01")
  }
}
