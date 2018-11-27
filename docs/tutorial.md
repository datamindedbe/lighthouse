---
layout: page
title: Tutorial
permalink: /tutorial/
---

In this tutorial, we'll build an example data lake and data pipeline based on airline traffic data published [here](http://stat-computing.org/dataexpo/2009/the-data.html). You can already download and unpack those files. One year is more than enough. You can also quickly grab the data from our [repo](https://github.com/datamindedbe/lighthouse/tree/master/lighthouse-demo/src/main/resources/data).

Steps:
* Do not remove this line (it will not be displayed)
{:toc}
 
### Install Lighthouse
Start by adding the Lighthouse dependency to your project. In case you are using maven:
```xml
<dependency>
    <groupId>be.dataminded</groupId>
    <artifactId>lighthouse_2.11</artifactId>
    <version>0.2.5</version>
</dependency>
```
or sbt:

```scala
libraryDependencies += "be.dataminded" %% "lighthouse" % "0.2.5"
```
You can also manually clone and build Lighthouse. You can get the repo here:
```
git clone git@github.com:datamindedbe/lighthouse.git
```

### Define the data in your data lake
A data lake is a collection of different data sources that we bring together in one place. Building a data lake in Lighthouse means writing Scala code. This has several advantages over more tradtional, statically defined, config files. For one, it allows for more flexibility, which you will see below. Secondly, with your data lake being code, it can go through the same code review and test process as all the rest of your code.  

You need three things to define a data source:
 1. **DataLink**: This is a pointer to where the data is stored. More often than not, it's a link to the file system on which you store your data lake (local file system, AWS S3, Azure Blob Storage, Google Cloud Storage, ...). But it could also be a `JDBCDataLink`, connecting to a SQL database. We'll discuss later all the options available.
 2. **DataUID**: This uniquely identifies a data source in the data lake. A `DataUID` has 2 parameters: a `namespace` and a `key`. You can think of the namespace as the zone of your data lake where this data belongs, and the key as the actual name of the `DataLink`. How you structure your data lake in zones, is out-of-scope for this tutorial. 
 3. **Environment**: Your local development environment will (hopefully) look different from your production environment. In Lighthouse, you can define several environments and add a `DataLink` to one or more environments, depending on your needs. Since all this configuration is code, you can be very flexible in bringing together `DataLinks`, `DataUIDs` and `environments`. Usually, you have a local `dev` or `test` environment, an `acceptance` environment and a `prod` environment. The local environment can be run from your laptop, while the `acceptance` and `prod` environments can run in the cloud. 
 
Let's begin with defining some of those data sources. The biggest one is the airplane data itself. For this simple tutorial we define a `FileSystemDataLink` to the folder where we stored the file locally, as such:

```scala
val datalink = new FileSystemDataLink(
  resource("data/airplane"),
  Csv,
  SaveMode.ErrorIfExists,
  options = Map("header" -> "true", "inferSchema" -> "true"))
``` 

This is telling Lighthouse: You will find a csv file in the folder `data/airplane`. The csv file has headers. You can infer the schema yourself. Now, we need to have a unique way to refer to this `DataLink`. The way we do that is by linking the `DataLink` to a `DataUID` which has 2 parameters: a `namespace` and a `key`. You can think of the namespace as the zone of your data lake where this data belongs, and the key as the actual name of the `DataLink`. So for this raw airplane data, we could define the `DataUID` as follows:

```scala
val dataUID = DataUID("raw", "airplane")
```
This is telling Lighthouse: I have an `airplane` dataset, in the `raw` zone of the data lake. 
All data sources coming from the outside world are considered `raw`. Which means it can be any format: csv, json, avro, gzip, ... Later on, we will transform those data sources into a structured format, by default `ORC`.

Finally, we are ready to define a simple test environment with that one data source:
```scala
 environment("test") { refs =>
    refs += DataUID("raw", "airplane") -> new FileSystemDataLink(
      resource("data/airplane"),
      Csv,
      SaveMode.ErrorIfExists,
      options = Map("header" -> "true", "inferSchema" -> "true"))
 }
```
Congratulations, you have built your first data lake. It has one environment: `test`, which contains one `DataLink`: a csv file loaded from your local disk. You cataloged that data to be in the `raw` zone of your data lake and it is named `airplane`. Let's extend this with two weather sources, in a similar fashion, and wrap it all in our very own `AirplaneDatalake` object.

```scala
object AirplaneDatalake extends Datalake {
     environment("test") { refs =>
        refs += DataUID("raw", "airplane") -> new FileSystemDataLink(
          resource("data/airplane"),
          Csv,
          SaveMode.ErrorIfExists,
          options = Map("header" -> "true", "inferSchema" -> "true"))
          
        refs += DataUID("raw.weather", "daily") -> new FileSystemDataLink(
            resource("data/weather/daily"),
            Csv,
            SaveMode.ErrorIfExists,
            options = Map("header" -> "true", "inferSchema" -> "true"))
        
        refs += DataUID("raw.weather", "station") -> new FileSystemDataLink(
            resource("data/weather/station"),
            Csv,
            SaveMode.ErrorIfExists,
            options = Map(
              "header" -> "true", 
              "inferSchema" -> "true", 
              "delimiter"   -> "|"))
     }
     
     private def resource(path: String): String = File.resource(path).pathAsString
 }
``` 

Great, now how can you use this? Well, from this moment onwards, you can always access the data sources by just asking the data lake:

```scala
    val df = AirplaneDatalake("raw" -> "airplane").read()
```
This allows you to define all the data sources in your data lake in one single location, and you can use them wherever you need them. You don't need to worry about which format it's in, the exact delimiter to set, ... That's all abstracted away from you. And while this does not add a lot of value if it's just you and a small team, doing a very specific analysis, this can be useful in medium to large organisations, where you have multiple teams of engineers working on the data lake, there is high reuse of data and data scientists just want quick access to all available data. 
 
### Build a data pipeline
Having a lot of data in your data lake is only valuable once you start doing things with it. This usually involves building some sort of pipeline. There's nothing wrong with simply calling a bunch of Spark transformations in a `run()` function, and simply executing it. And Lighthouse allows you to do exactly that. 

However, in larger projects, this can quickly result in very long, hard to understand, hard to test, and hard to reuse pipelines. So how can we improve on this? By using the `Pipeline` library available in Lighthouse. Let's look at an example:

```scala
val airlines: SparkFunction[DataFrame] =
    Sources
      .fromDataLink(AirplaneDatalake("raw" -> "airplane"))
      .flatMap(cleanAirlines)
      .write(SingleFileSink(AirplaneDatalake("clean" -> "airplane")))
```
This is a very simple pipeline, that reads the raw `airplane` data, that we defined above, cleans it in the `cleanAirlines` function, and then writes it back to the location `clean -> airplaine` in the data lake. But wait, we haven't defined the clean airplanes yet. Let's do that first. Since we can control how this data source looks like, we can just choose the defaults, which is using the `ORC` format. You can add the following line in your data lake definition:

```scala
    refs += DataUID("clean", "airplane") -> 
      new FileSystemDataLink(file"target/clean/airplane".pathAsString)
```
Now the only thing left to do, is to define the `cleanAirlines` function:

```scala
  private def cleanAirlines(airlines: DataFrame) = SparkFunction { spark =>
    import spark.implicits._

    val timestamp = udf((year: String, month: String, day: String) => 
      f"$year${month.toInt}%02d${day.toInt}%02d".toInt)

    airlines
      .select('Origin, 'Dest, 'Year, 'Month, 'DayofMonth, 'DayOfWeek, 'ArrDelay)
      .withColumn("YearMonthDay", timestamp('Year, 'Month, 'DayofMonth))
  }
```
Congratulations, you have built your first pipeline. Why is this helpful? Well, there are a couple of advantages over writing large all-in-one `run()` functions:
1. You've separated reading and writing from the transformation logic. That means that that transformation logic can be tested independently. 
2. You use the data lake for reading and writing. You don't need to worry about how the data lake gets that data. You've solved that problem once, in the definition of the data lake. You can do 100 more projects using this airline data. 
3. You've built a separate cleaning step. That's a good thing. If you systematically clean all data sources that come in to your data lake, you can enforce a uniform, efficient structure. By default, this is `ORC`. But you can also use `parquet` or other file formats. You can also, optionally, log data quality issues here. 

We'll add cleaning steps for the other two data sources, as such:

```scala
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
```

Now we also need to add the two cleaned data sources to the data lake, because you're referencing them above:
```scala
refs += DataUID("clean", "weather")  -> 
  new FileSystemDataLink(file"target/clean/weather/daily".pathAsString)
refs += DataUID("clean", "stations") -> 
  new FileSystemDataLink(file"target/clean/weather/stations".pathAsString)
```

The actual implementation of the cleaning functions, you can find in the code base of Lighthouse, and don't really matter for this demo. What is cool however, is that now we can reuse those clean data sources everywhere. Let's build a single view for instance. First we join both weather data sources:

```scala
  val weatherWithStations: SparkFunction[DataFrame] = for {
    weather  <- dailyWeather
    stations <- weatherStations
  } yield dailyWeatherWithStation(weather, stations).cache()
  
  private def dailyWeatherWithStation(dailyWeather: DataFrame, weatherStations: DataFrame) = {
      dailyWeather.join(weatherStations, "WBAN").drop("WBAN")
  }
```

Next, we combine this with the original airline data and we finally write it back to the data lake. 
```scala
  val pipeline: SparkFunction[DataFrame] =
    (airlines, weatherWithStations)
      .mapN(buildView)
      .write(AirplaneDatalake("master" -> "view"))
  
  private def buildView(airlines: DataFrame, dailyWeatherWithStation: DataFrame): 
    DataFrame = {
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
```

For this to work, we need to add the `master -> view` object to the data lake definition:


```scala
    refs += DataUID("master", "view") -> new HiveDataLink(
      file"target/master/airplane/view".pathAsString,
      "default",
      "airplane_view")
```
Note that this time, we made it a `HiveDataLink` and we store it in the table `airplane_view` of the database `default`. That means that any consumer downstream, such as a data science process that tries to predict delays based on this integrated airline data, can simply read it from Hive. 

 
### Demo
You can find the full source code of this example in the `lighthouse-demo` folder of the Lighthouse repository. [Unit tests](https://github.com/datamindedbe/lighthouse/tree/master/lighthouse-core/src/test) are available to show how each aspect is supposed to work, on your own machine. As Lighthouse is just a Spark library, you can deploy it in your normal deployment pipeline, you can include it in your jar, and simply submit Spark jobs to your cluster. 