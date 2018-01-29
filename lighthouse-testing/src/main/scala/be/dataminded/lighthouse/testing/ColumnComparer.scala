package be.dataminded.lighthouse.testing

import org.apache.spark.sql.DataFrame

trait ColumnComparer {

  def assertColumnEquality(dataFrame: DataFrame, left: String, right: String): Unit = {
    val elements      = dataFrame.select(left, right).collect()
    val leftElements  = elements.map(_(0))
    val rightElements = elements.map(_(1))

    assert(
      leftElements.sameElements(rightElements),
      s"""
         |Columns aren't equal
         |'$left' elements:
         |[${leftElements.mkString(", ")}]
         |'$right' elements:
         |[${rightElements.mkString(", ")}]
       """.stripMargin
    )
  }
}
