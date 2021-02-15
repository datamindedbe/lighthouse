package be.dataminded.lighthouse.testing

import java.sql.Date

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import java.time.format.DateTimeFormatter

private[testing] object DataFramePrettyPrinter {

  def prettyPrintDataFrame(df: DataFrame, number: Int, truncate: Int = 20): String = {
    val numRows     = number.max(0)
    val takeResult  = df.take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data        = takeResult.take(numRows)

    val header = df.schema.fieldNames.toSeq

    // For array values, replace Seq and Array with square brackets.
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "...".
    def asReadableRows = {
      data.map { row =>
        row.toSeq.view.map { cell =>
          val str = cell match {
            case null                => "null"
            case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
            case seq: Traversable[_] => seq.mkString("[", ", ", "]")
            case d: Date             => DateTimeFormatter.ISO_LOCAL_DATE.format(d.toLocalDate())
            case _                   => cell.toString
          }

          if (truncate > 0 && str.length > truncate) {
            // do not show ellipses for strings shorter than 4 characters.
            if (truncate < 4) str.substring(0, truncate)
            else str.substring(0, truncate - 3) + "..."
          } else {
            str
          }
        }: Seq[String]
      }
    }

    val rows = (header +: asReadableRows).view
    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(header.length)(3)

    // Compute the maximal width of each column.
    for {
      row       <- rows
      (cell, i) <- row.zipWithIndex
    } colWidths(i) = math.max(colWidths(i), cell.length)

    // (rows + 3 separator lines + has more rows) * (total content + separators + ending)
    val sb = new StringBuilder((numRows + 4) * (colWidths.sum + numRows + 2))
    // Create separator line.
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()
    // Choose padding implementation.
    val pad: (String, Int) => String = if (truncate > 0) StringUtils.leftPad else StringUtils.rightPad

    (for {
      row       <- rows
      (cell, i) <- row.zipWithIndex
    } yield pad(cell, colWidths(i)))
      .grouped(colWidths.size)
      .foreach(_.addString(sb, "|", "|", "|\n"))
    // Add separators after the header and after the last row.
    sb.insert(sep.length() * 2, sep)
    sb.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }
}
