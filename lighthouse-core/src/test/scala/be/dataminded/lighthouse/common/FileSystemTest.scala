package be.dataminded.lighthouse.common

import java.io.FileNotFoundException

import better.files.File
import org.scalatest.{FunSuite, Matchers}

class FileSystemTest extends FunSuite with Matchers {

  test("Can read a file as a string") {
    val content = FileSystem.read(File.resource("customers.csv").pathAsString)
    content should not be empty
  }

  test("Throws exception when file does not exist") {
    an[FileNotFoundException] should be thrownBy FileSystem.read("/unknown.csv")
  }

}
