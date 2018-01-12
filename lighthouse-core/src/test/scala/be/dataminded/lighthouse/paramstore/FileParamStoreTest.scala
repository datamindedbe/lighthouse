package be.dataminded.lighthouse.paramstore

import java.io.FileNotFoundException

import org.scalatest.{FunSuite, Matchers}

class FileParamStoreTest extends FunSuite with Matchers {

  test("Can be used to retrieve a property from file") {
    //TODO: Implement
  }

  test("Non existing file trows an exception") {
    //TODO: Implement
  }

  test("Non existing config path trows an exception") {
    //TODO: Implement
  }

  test("Non existing key trows an exception") {
    //TODO: Implement
  }

  test("Validation happens at call, not at retrieval") {
    val store    = new FileParamStore("/some/unexisting/file/path")
    val function = store.lookupFunction("/some/config/path", "some/key")

    an[FileNotFoundException] should be thrownBy function.apply()
  }
}

