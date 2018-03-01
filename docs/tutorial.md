---
layout: page
title: Tutorial
permalink: /tutorial/
---

In this tutorial, we'll build an example data lake and data pipeline based on airline traffic data published [here](http://stat-computing.org/dataexpo/2009/the-data.html). You can already download and unpack those files. One year is more than enough. You can also quickly grab the data from our [repo](https://github.com/datamindedbe/lighthouse/tree/master/lighthouse-demo/src/main/resources/data). 
### Install Lighthouse
Start by adding the Lighthouse dependency to your project. In case you are using maven:
```xml
<dependency>
    <groupId>be.dataminded</groupId>
    <artifactId>lighthouse_2.11</artifactId>
    <version>0.2.0</version>
</dependency>
```
or sbt:

```scala
libraryDependencies += "be.dataminded" % "lighthouse_2.11" % "0.2.0"
```
You can also manually clone and build Lighthouse. You can get the repo here:
```
git clone git@github.com:datamindedbe/lighthouse.git
```

### Define the data in your data lake
A data lake is a collection of different data sources that we bring together in one place. Building a data lake in Lighthouse means writing Scala code. This has several advantages over more tradtional, statically defined, config files. For one, it allows for more flexibility, which you will see below. Secondly, with your data lake being code, it can go through the same code review and test process as all the rest of your code.  

You need three things to define a data source:
 1. a **DataLink**: this is a pointer to where the data is stored. More often than not, it's a link to the file system on which you store your data lake (local file system, AWS S3, Azure Blob Storage, Google Cloud Storage, ...). But it could also be a `JDBCDataLink`, connecting to a SQL database. We'll discuss later all the options available.
 2. a **DataUID**: this uniquely identifies a data source in the data lake. A `DataUID` has 2 parameters: a `namespace` and a `key`. You can think of the namespace as the zone of your data lake where this data belongs, and the key as the actual name of the `DataLink`.
 3. an **environment**: your local development environment will (hopefully) look different from your production environment. In Lighthouse, you can define several environments and add a `DataLink` to one or more environments, depending on your needs. 
 
Since all this configuration is code, you can be very flexible in bringing together `DataLinks`, `DataUIDs` and `environments`. Let's begin with defining some of those data sources. The biggest one is the airplane data itself. For this simple tutorial we define a `FileSystemDataLink` to the folder where we stored the file locally, as such:

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
Congratulations, you have built your first data lake. It has one environment: `test`, which contains one `DataLink`: a csv file loaded from your local disk. You cataloged that data to be in the `raw` zone of your data lake and it is named `airplane`. 