import Dependencies._
import Resolvers._
import sbt.Opts.resolver.sonatypeStaging

lazy val buildSettings = Seq(
  organization := "be.dataminded",
  scalaVersion := "2.11.12",
  // Ensure code quality
  scalafmtOnCompile := true,
  // Memory settings to be able to test with Spark
  Test / fork := true,
  Test / testOptions += Tests.Argument("-oD"),
  javaOptions ++= Seq(
    "-Xms768M",
    "-Xmx2048M",
    "-XX:+CMSClassUnloadingEnabled",
    "-Dspark.sql.shuffle.partitions=2",
    "-Dspark.shuffle.sort.bypassMergeThreshold=2",
    "-Dlighthouse.environment=test"
  ),
  scalacOptions ++= Seq("-Ypartial-unification", "-Ywarn-value-discard"),
  // Git versioning
  git.useGitDescribe := true,
  git.baseVersion := "0.0.0",
  // Publish like Maven
  publishMavenStyle := true,
  publishTo := Some(if (isSnapshot.value) datamindedSnapshots else sonatypeStaging),
  homepage := Some(url("https://github.com/datamindedbe/lighthouse")),
  scmInfo := Some(
    ScmInfo(url("https://github.com/datamindedbe/lighthouse"), "git@github.com:datamindedbe/lighthouse.git")),
  developers := List(
    Developer("mlavaert", "Mathias Lavaert", "mathias.lavaert@dataminded.be", url("https://github.com/mlavaert"))),
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
)

lazy val `lighthouse-platform` = (project in file("."))
  .settings(buildSettings, publishArtifact := false)
  .enablePlugins(GitVersioning)
  .aggregate(lighthouse, `lighthouse-testing`, `lighthouse-demo`)

lazy val lighthouse = (project in file("lighthouse-core"))
  .dependsOn(`lighthouse-testing` % "test->compile")
  .enablePlugins(SiteScaladocPlugin)
  .settings(buildSettings, libraryDependencies ++= commonDependencies ++ Seq(cats, typesafeConfig))

lazy val `lighthouse-testing` = (project in file("lighthouse-testing"))
  .settings(buildSettings, libraryDependencies ++= Seq(sparkSql, sparkHive, scalaTest, betterFiles))

lazy val `lighthouse-demo` = (project in file("lighthouse-demo"))
  .dependsOn(lighthouse, `lighthouse-testing` % "test->compile")
  .settings(buildSettings, publishArtifact := false, libraryDependencies ++= commonDependencies)
