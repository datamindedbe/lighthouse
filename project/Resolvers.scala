import sbt._

object Resolvers {

  private val `dataminded-artifacts` = "s3://s3-eu-west-1.amazonaws.com/dataminded-artifacts/maven"

  lazy val datamindedSnapshots = "dataminded-releases" at s"${`dataminded-artifacts`}/snapshot"

  private val `goodyear-artifacts` = "https://nexus.goodyear.eu/repository"

  lazy val gyReleases = "goodyear-releases" at s"${`goodyear-artifacts`}/maven-releases"

}
