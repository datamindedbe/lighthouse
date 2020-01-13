package be.dataminded.lighthouse.datalake

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LazyConfigTest extends AnyFunSuite with Matchers {

  test("LazyConfig should be evaluated every time is being called") {
    val test: LazyConfig[Long] = System.nanoTime

    val firstCall  = test()
    val secondCall = test()

    firstCall should not equal secondCall
  }

  test("LazyConfig can be created using a factory method that takes any value") {
    val test: LazyConfig[Long] = LazyConfig(1L)

    test() should equal(1L)
  }

  test("LazyConfig can be created using a factory method with a body") {
    val test: LazyConfig[Long] = LazyConfig {
      System.out.println("Loading lazy value")
      System.nanoTime()
    }

    test() should not equal test()
  }

  test("LazyConfig can be created from a value using the help of an implicit conversion") {
    val test: LazyConfig[Long] = 1L

    test() should equal(1L)
  }
}
