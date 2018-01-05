import sbt._

object Dependencies {

  lazy val sparkCore = "org.apache.spark" %% "spark-core"  % "2.2.1" % Provided
  lazy val sparkSql  = "org.apache.spark" %% "spark-sql"   % "2.2.1" % Provided
  lazy val sparkHive = "org.apache.spark" %% "spark-hive"  % "2.2.1" % Provided
  lazy val sparkMlib = "org.apache.spark" %% "spark-mllib" % "2.2.1" % Provided

  lazy val scopt          = "com.github.scopt"           %% "scopt"          % "3.7.0"
  lazy val amazonSdk      = "com.amazonaws"              % "aws-java-sdk-s3" % "1.11.229"
  lazy val typesafeConfig = "com.typesafe"               % "config"          % "1.3.2"
  lazy val logback        = "ch.qos.logback"             % "logback-classic" % "1.2.3"
  lazy val scalaLogging   = "com.typesafe.scala-logging" %% "scala-logging"  % "3.7.2"

  lazy val cats        = "org.typelevel"        %% "cats-core"    % "1.0.1"
  lazy val betterFiles = "com.github.pathikrit" %% "better-files" % "3.4.0"

  lazy val scalaTest        = "org.scalatest"   %% "scalatest"          % "3.0.3"
  lazy val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0"

  lazy val commonDependencies =
    Seq(sparkCore, sparkSql, sparkHive, scopt, betterFiles, scalaLogging, scalaTest % Test)
}
