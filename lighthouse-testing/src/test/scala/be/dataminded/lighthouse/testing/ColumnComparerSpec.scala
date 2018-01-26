package be.dataminded.lighthouse.testing

import org.apache.spark.sql.functions._
import org.scalatest.FunSpec

class ColumnComparerSpec extends FunSpec with SharedSparkSession with ColumnComparer {

  import spark.implicits._

  describe("assertColumnEquality") {

    it("doesn't do anything if all the column values are equal") {
      val source = Seq((1, 1), (5, 5)).toDF("num", "expected")

      assertColumnEquality(source, "num", "expected")
    }

    it("throws an error if the columns aren't equal") {
      val source = Seq((1, 3), (5, 5)).toDF("num", "expected")

      intercept[AssertionError] {
        assertColumnEquality(source, "num", "expected")
      }
    }

    it("throws an error if the columns are different types") {
      val source = Seq((1, "hi"), (5, "bye")).toDF("num", "word")

      intercept[AssertionError] {
        assertColumnEquality(source, "num", "word")
      }
    }

    it("works properly, even when null is compared with a value") {
      val source = Seq((1, 1), (null.asInstanceOf[Int], 5), (null.asInstanceOf[Int], null.asInstanceOf[Int]))
        .toDF("num", "expected")

      intercept[AssertionError] {
        assertColumnEquality(source, "num", "expected")
      }
    }

    it("works for ArrayType columns") {
      val source =
        Seq((Array("a"), Array("a")), (Array("a", "b"), Array("a", "b"))).toDF("left", "right")
      assertColumnEquality(source, "left", "right")
    }

    it("works for computed ArrayType columns") {
      val source = Seq(
        ("i like blue and red", Array("blue", "red")),
        ("you pink and blue", Array("blue", "pink")),
        ("i like fun", Array(""))
      ).toDF("words", "expected_colors")

      val actual = source.withColumn(
        "colors",
        split(
          concat_ws(
            ",",
            when(col("words").contains("blue"), "blue"),
            when(col("words").contains("red"), "red"),
            when(col("words").contains("pink"), "pink"),
            when(col("words").contains("cyan"), "cyan")
          ),
          ","
        )
      )
      assertColumnEquality(actual, "colors", "expected_colors")
    }
  }
}
