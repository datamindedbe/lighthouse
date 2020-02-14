# Lighthouse
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/be.dataminded/lighthouse_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/be.dataminded/lighthouse)
[![CI](https://github.com/mrgambal/lighthouse/workflows/CI/badge.svg)](https://github.com/mrgambal/lighthouse/actions?query=workflow%3ACI)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/a0cb9f75da0a4df887b06d37434cfc04)](https://www.codacy.com/app/mLavaert/lighthouse?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=datamindedbe/lighthouse&amp;utm_campaign=Badge_Grade)

Lighthouse is a library for data lakes built on top of [Apache Spark](http://spark.apache.org/). 
It provides high-level APIs in Scala to streamline data pipelines and apply best practices. 

## Principles

- Configuration as code
- Idempotent execution
- Utilities for easier building and testing Apache Spark based applications

## Start using Lighthouse

In your `build.sbt`, add this:
```scala
libraryDependencies += "be.dataminded" %% "lighthouse" % <version>
libraryDependencies += "be.dataminded" %% "lighthouse-testing" % <version> % Test
```

If you are using Maven, add this to your `pom.xml`: 

```xml
<dependency>
    <groupId>be.dataminded</groupId>
    <artifactId>lighthouse_2.11</artifactId>
    <version>[version]</version>
</dependency>
<dependency>
    <groupId>be.dataminded</groupId>
    <artifactId>lighthouse-testing_2.11</artifactId>
    <version>[version]</version>
    <scope>test</scope>
</dependency>
```

## Online Documentation

This README file only contains basic instructions. Here is a more complete tutorial: https://datamindedbe.github.io/lighthouse/tutorial/
