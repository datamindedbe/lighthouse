import sbt._

object Dependencies {

  private lazy val amazonSdkVersion = "1.11.229"
  private lazy val sparkVersion     = "2.2.1"

  lazy val sparkCore = "org.apache.spark" %% "spark-core"  % sparkVersion % Provided
  lazy val sparkSql  = "org.apache.spark" %% "spark-sql"   % sparkVersion % Provided
  lazy val sparkHive = "org.apache.spark" %% "spark-hive"  % sparkVersion % Provided
  lazy val sparkMlib = "org.apache.spark" %% "spark-mllib" % sparkVersion  % Provided

  lazy val scopt          = "com.github.scopt"           %% "scopt"          % "3.7.0"
  lazy val typesafeConfig = "com.typesafe"               % "config"          % "1.3.2"
  lazy val logback        = "ch.qos.logback"             % "logback-classic" % "1.2.3"
  lazy val scalaLogging   = "com.typesafe.scala-logging" %% "scala-logging"  % "3.7.2"

  lazy val cats        = "org.typelevel"        %% "cats-core"    % "1.0.1"
  lazy val betterFiles = "com.github.pathikrit" %% "better-files" % "3.4.0"

  lazy val scalaTest        = "org.scalatest"   %% "scalatest"          % "3.0.3"
  lazy val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0"

  // AWS library dependencies
  lazy val amazonSdkS3  = "com.amazonaws" % "aws-java-sdk-s3"  % amazonSdkVersion
  lazy val amazonSdkSSM = "com.amazonaws" % "aws-java-sdk-ssm" % amazonSdkVersion

  lazy val commonDependencies = Seq(sparkCore, sparkSql, sparkHive, scopt, betterFiles, scalaLogging, scalaTest % Test)

  lazy val amazonSdk = Seq(amazonSdkS3, amazonSdkSSM)
}
