addSbtPlugin("com.typesafe.sbt"   % "sbt-git"            % "1.0.0")
addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.14.0")
addSbtPlugin("com.geirsson"       % "sbt-scalafmt"       % "1.4.0")
addSbtPlugin("io.get-coursier"    % "sbt-coursier"       % "1.0.0")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"      % "1.5.1")
addSbtPlugin("com.typesafe.sbt"   % "sbt-site"           % "1.3.2")

// Publish to Maven Central
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.0")
addSbtPlugin("com.jsuereth"   % "sbt-pgp"      % "1.1.2")
