package be.dataminded.lighthouse.demo

import be.dataminded.lighthouse.datalake._
import be.dataminded.lighthouse.pipeline._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait AirplanePipeline {

  val airlines: SparkFunction[DataFrame] =
    Sources
      .fromDataLink(AirplaneDatalake("raw" -> "airplane"))
      .flatMap(cleanAirlines)
      .write(SingleFileSink(AirplaneDatalake("clean" -> "airplane")))

  val dailyWeather: SparkFunction[DataFrame] =
    Sources
      .fromDataLink(AirplaneDatalake("raw.weather" -> "daily"))
      .flatMap(cleanDailyWeather)
      .write(SingleFileSink(AirplaneDatalake("clean" -> "weather")))

  val weatherStations: SparkFunction[DataFrame] =
    Sources
      .fromDataLink(AirplaneDatalake("raw.weather" -> "station"))
      .flatMap(cleanWeatherStations)
      .write(SingleFileSink(AirplaneDatalake("clean" -> "stations")))

  val weatherWithStations: SparkFunction[DataFrame] = for {
    weather  <- dailyWeather
    stations <- weatherStations
  } yield dailyWeatherWithStation(weather, stations).cache()

  val pipeline: SparkFunction[DataFrame] =
    (airlines, weatherWithStations)
      .mapN(buildView)
      .write(AirplaneDatalake("master" -> "view"))

  private def cleanDailyWeather(dailyWeather: DataFrame) =
    SparkFunction { spark =>
      import spark.implicits._

      val toCelsius = udf((temp: Int) => ((temp - 32) / 1.8).toInt)

      dailyWeather
        .select('WBAN, 'YearMonthDay, 'Tmin, 'Tmax, 'Tavg, 'PrecipTotal, 'AvgSpeed)
        .withColumn("Tmin", toCelsius('Tmin))
        .withColumn("Tmax", toCelsius('Tmax))
        .withColumn("Tavg", toCelsius('Tavg))
        .withColumnRenamed("AvgSpeed", "WAvgSpeed")
    }

  protected def cleanWeatherStations(weatherStations: DataFrame) =
    SparkFunction { spark =>
      import spark.implicits._

      weatherStations.select('WBAN, 'CallSign).withColumnRenamed("CallSign", "IAT").distinct()
    }

  private def cleanAirlines(airlines: DataFrame) =
    SparkFunction { spark =>
      import spark.implicits._

      val timestamp =
        udf((year: String, month: String, day: String) => f"$year${month.toInt}%02d${day.toInt}%02d".toInt)

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
          "YearMonthDay"
        ),
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
          "YearMonthDay"
        ),
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
