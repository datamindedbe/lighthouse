import Dependencies._
import Resolvers.{datamindedReleases, datamindedSnapshots}

lazy val buildSettings = Seq(
  organization := "be.dataminded",
  scalaVersion := "2.11.12",
  // Ensure code quality
  scalafmtOnCompile := true,
  wartremoverWarnings in (Compile, compile) ++= Warts.allBut(Wart.DefaultArguments, Wart.FinalCaseClass, Wart.Throw),
  // Memory settings to be able to test with Spark
  fork in Test := true,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
  scalacOptions ++= Seq("-Ypartial-unification"),
  // Git versioning
  git.useGitDescribe := true,
  git.baseVersion := "0.0.0",
  // Publish like Maven
  publishTo := Some(if (isSnapshot.value) datamindedSnapshots else datamindedReleases),
  publishMavenStyle := true,
  pomExtra :=
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
)

lazy val `lighthouse-platform` = (project in file("."))
  .settings(buildSettings)
  .enablePlugins(GitVersioning)
  .aggregate(`lighthouse-core`, `lighthouse-testing`)

lazy val `lighthouse-core` = (project in file("lighthouse-core"))
  .dependsOn(`lighthouse-testing` % "test->compile")
  .settings(buildSettings, libraryDependencies ++= commonDependencies ++ Seq(cats))

lazy val `lighthouse-testing` = (project in file("lighthouse-testing"))
  .settings(buildSettings, libraryDependencies ++= Seq(sparkSql, sparkHive, scalaTest, sparkTestingBase))