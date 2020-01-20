package be.dataminded.lighthouse.common

import java.nio.file.NoSuchFileException

import better.files.Resource
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FileSystemTest extends AnyFunSuite with Matchers {

  test("Can read a file as a string") {
    val content = FileSystem.read(Resource.getUrl("customers.csv").getPath())
    content should not be empty
  }

  test("Throws exception when file does not exist") {
    an[NoSuchFileException] should be thrownBy FileSystem.read("/unknown.csv")
  }
}
