import Dependencies._
import Opts.resolver.sonatypeSnapshots
import Opts.resolver.sonatypeStaging

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
  scalacOptions ++= Seq("-Ypartial-unification"),
  // Git versioning
  git.useGitDescribe := true,
  git.baseVersion := "0.0.0",
  // Publish like Maven
  publishMavenStyle := true,
  publishTo := Some(if (isSnapshot.value) sonatypeSnapshots else sonatypeStaging),
  homepage := Some(url("https://lighthouse.dataminded.be")),
  scmInfo := Some(ScmInfo(url("https://github.com/dataminded/lighthouse"), "git@github.com:dataminded/lighthouse.git")),
  developers := List(
    Developer("mlavaert", "Mathias Lavaert", "mathias.lavaert@dataminded.be", url("https://github.com/mlavaert"))),
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
)

lazy val `lighthouse-platform` = (project in file("."))
  .settings(buildSettings)
  .enablePlugins(GitVersioning)
  .aggregate(`lighthouse-core`, `lighthouse-testing`, `lighthouse-demo`)

lazy val `lighthouse-core` = (project in file("lighthouse-core"))
  .dependsOn(`lighthouse-testing` % "test->compile")
  .settings(buildSettings, libraryDependencies ++= commonDependencies ++ amazonSdk ++ Seq(cats, typesafeConfig))

lazy val `lighthouse-testing` = (project in file("lighthouse-testing"))
  .settings(buildSettings, libraryDependencies ++= Seq(sparkSql, sparkHive, scalaTest, betterFiles))

lazy val `lighthouse-demo` = (project in file("lighthouse-demo"))
  .dependsOn(`lighthouse-core` , `lighthouse-testing` % "test->compile")
  .settings(buildSettings, libraryDependencies ++= commonDependencies)
