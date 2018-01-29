package be.dataminded.lighthouse.demo

import be.dataminded.lighthouse.datalake._
import be.dataminded.lighthouse.pipeline._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait AirplanePipeline {

  val airlines: SparkFunction[DataFrame] = Sources
    .fromDataLink(AirplaneDatalake("raw" -> "airplane"))
    .flatMap(cleanAirlines)
    .makeSnapshot(SingleFileSink(AirplaneDatalake("clean" -> "airplane")))

  val dailyWeather: SparkFunction[DataFrame] = Sources
    .fromDataLink(AirplaneDatalake("raw.weather" -> "daily"))
    .flatMap(cleanDailyWeather)
    .makeSnapshot(SingleFileSink(AirplaneDatalake("clean.weather" -> "daily")))

  val weatherStations: SparkFunction[DataFrame] = Sources
    .fromDataLink(AirplaneDatalake("raw.weather" -> "station"))
    .flatMap(cleanWeatherStations)
    .makeSnapshot(SingleFileSink(AirplaneDatalake("clean.weather" -> "station")))

  val pipeline: SparkFunction[DataFrame] =
    (airlines, (dailyWeather, weatherStations).mapN(dailyWeatherWithStation).cache())
      .mapN(buildView)
      .makeSnapshot(AirplaneDatalake("master.airplane" -> "view"))

  private def cleanDailyWeather(dailyWeather: DataFrame) = SparkFunction { spark =>
    import spark.implicits._

    val toCelcius = udf((temp: Int) => ((temp - 32) / 1.8).toInt)

    dailyWeather
      .select('WBAN, 'YearMonthDay, 'Tmin, 'Tmax, 'Tavg, 'PrecipTotal, 'AvgSpeed)
      .withColumn("Tmin", toCelcius('Tmin))
      .withColumn("Tmax", toCelcius('Tmax))
      .withColumn("Tavg", toCelcius('Tavg))
      .withColumnRenamed("AvgSpeed", "WAvgSpeed")
  }

  private def cleanWeatherStations(weatherStations: DataFrame) = SparkFunction { spark =>
    import spark.implicits._

    weatherStations.select('WBAN, 'CallSign).withColumnRenamed("CallSign", "IAT").distinct()
  }

  private def cleanAirlines(airlines: DataFrame) = SparkFunction { spark =>
    import spark.implicits._

    val timestamp = udf((year: String, month: String, day: String) => f"$year${month.toInt}%02d${day.toInt}%02d".toInt)

    airlines
      .select('Origin, 'Dest, 'Year, 'Month, 'DayofMonth, 'DayOfWeek, 'ArrDelay)
      .withColumn("YearMonthDay", timestamp('Year, 'Month, 'DayofMonth))
  }

  private def dailyWeatherWithStation(dailyWeather: DataFrame, weatherStations: DataFrame) = {
    dailyWeather.join(weatherStations, "WBAN").drop("WBAN")
  }

  private def buildView(airlines: DataFrame, dailyWeatherWithStation: DataFrame): DataFrame = {
    airlines
      .join(
        dailyWeatherWithStation,
        airlines("Origin") === dailyWeatherWithStation("IAT") && airlines("YearMonthDay") === dailyWeatherWithStation(
          "YearMonthDay"),
        "left_outer"
      )
      .drop("IAT")
      .drop(dailyWeatherWithStation("YearMonthDay"))
      .withColumnRenamed("Tmin", "TminOrigin")
      .withColumnRenamed("Tmax", "TmaxOrigin")
      .withColumnRenamed("Tavg", "TavgOrigin")
      .withColumnRenamed("PrecipTotal", "PrecipTotalOrigin")
      .withColumnRenamed("WavgSpeed", "WavgSpeedOrigin")
      .join(
        dailyWeatherWithStation,
        airlines("Dest") === dailyWeatherWithStation("IAT") && airlines("YearMonthDay") === dailyWeatherWithStation(
          "YearMonthDay"),
        "left_outer"
      )
      .drop(dailyWeatherWithStation("YearMonthDay"))
      .withColumnRenamed("Tmin", "TminDest")
      .withColumnRenamed("Tmax", "TmaxDest")
      .withColumnRenamed("Tavg", "TavgDest")
      .withColumnRenamed("PrecipTotal", "PrecipTotalDest")
      .withColumnRenamed("WavgSpeed", "WavgSpeedDest")
  }
}
