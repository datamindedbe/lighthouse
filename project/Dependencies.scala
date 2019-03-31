import sbt._

object Dependencies {

  private val amazonSdkVersion = "1.11.477"
  private val sparkVersion     = "2.4.0"

  val sparkCore = "org.apache.spark" %% "spark-core"  % sparkVersion % Provided
  val sparkSql  = "org.apache.spark" %% "spark-sql"   % sparkVersion % Provided
  val sparkHive = "org.apache.spark" %% "spark-hive"  % sparkVersion % Provided
  val sparkMlib = "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided

  val scopt          = "com.github.scopt"           %% "scopt"          % "3.7.1"
  val typesafeConfig = "com.typesafe"               % "config"          % "1.3.3"
  val logback        = "ch.qos.logback"             % "logback-classic" % "1.2.3"
  val scalaLogging   = "com.typesafe.scala-logging" %% "scala-logging"  % "3.9.0"
  val cats           = "org.typelevel"              %% "cats-core"      % "1.5.0"
  val betterFiles    = "com.github.pathikrit"       %% "better-files"   % "3.4.0"

  // Database connectivity
  val scalikejdbc = "org.scalikejdbc" %% "scalikejdbc" % "3.3.2"
  val h2          = "com.h2database"  % "h2"           % "1.4.197" % Test

  // Amazon AWS
  val awsSdkS3  = "com.amazonaws" % "aws-java-sdk-s3" % amazonSdkVersion % Provided
  val awsSdkSsm = "com.amazonaws" % "aws-java-sdk-ssm" % amazonSdkVersion % Provided
  val amazonSdk = Seq(awsSdkS3, awsSdkSsm)

  val scalaTest        = "org.scalatest" %% "scalatest" % "3.0.5"
  val testDependencies = Seq(scalaTest % Test, h2)

  val commonDependencies: Seq[ModuleID] =
    Seq(sparkCore, sparkSql, sparkHive, scopt, betterFiles, scalaLogging, scalikejdbc) ++ amazonSdk ++ testDependencies

}
