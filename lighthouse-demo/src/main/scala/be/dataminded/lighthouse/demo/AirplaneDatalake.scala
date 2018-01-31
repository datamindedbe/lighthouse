package be.dataminded.lighthouse.demo

import be.dataminded.lighthouse.datalake._
import be.dataminded.lighthouse.spark.Csv
import better.files._
import org.apache.spark.sql.SaveMode

object AirplaneDatalake extends Datalake {

  environment("test") { refs =>
    refs += DataUID("raw", "airplane") -> new FileSystemDataLink(resource("data/airplane"),
                                                                 Csv,
                                                                 SaveMode.ErrorIfExists,
                                                                 options =
                                                                   Map("header" -> "true", "inferSchema" -> "true"))

    refs += DataUID("raw.weather", "daily") -> new FileSystemDataLink(resource("data/weather/daily"),
                                                                      Csv,
                                                                      SaveMode.ErrorIfExists,
                                                                      options = Map("header"      -> "true",
                                                                                    "inferSchema" -> "true"))

    refs += DataUID("raw.weather", "station") -> new FileSystemDataLink(resource("data/weather/station"),
                                                                        Csv,
                                                                        SaveMode.ErrorIfExists,
                                                                        options = Map("header"      -> "true",
                                                                                      "inferSchema" -> "true",
                                                                                      "delimiter"   -> "|"))

    refs += DataUID("clean", "airplane") -> new FileSystemDataLink(file"target/clean/airplane".pathAsString)
    refs += DataUID("clean", "weather")  -> new FileSystemDataLink(file"target/clean/weather/daily".pathAsString)
    refs += DataUID("clean", "stations") -> new FileSystemDataLink(file"target/clean/weather/stations".pathAsString)
    refs += DataUID("master", "view") -> new HiveDataLink(file"target/master/airplane/view".pathAsString,
                                                          "default",
                                                          "airplane_view")
  }

  private def resource(path: String): String = File.resource(path).pathAsString
}
