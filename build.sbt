name := "scuttlebutt-akka-persistence"

scalaVersion := "2.12.8"

resolvers += "jcenter" at "http://jcenter.bintray.com/"
resolvers += "apache-snapshots" at "https://dl.bintray.com/openlawbot/forks/"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence" % "2.5.21"
libraryDependencies += "com.typesafe.akka" %% "akka-persistence-query" % "2.5.21"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.21"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0.pr1"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0.pr1"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0.pr1"
libraryDependencies += "io.vertx" % "vertx-core" % "3.6.2"

libraryDependencies += "org.logl" % "logl-logl" % "0.4.0-24B971-snapshot"
libraryDependencies += "org.logl" % "logl-api" % "0.4.0-24B971-snapshot"

libraryDependencies += "com.github.jnr" % "jnr-ffi" % "2.1.9"
libraryDependencies += "com.github.jnr" % "jffi" % "1.2.18"

libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.9.8"

libraryDependencies += "org.openlaw" % "tuweni-scuttlebutt" % "0.11.0"
libraryDependencies += "org.openlaw" % "tuweni-scuttlebutt-rpc" % "0.11.0"
libraryDependencies += "org.openlaw" % "tuweni-scuttlebutt-handshake" % "0.11.0"
libraryDependencies += "org.openlaw" % "tuweni-crypto" % "0.11.0"

updateOptions := updateOptions.value.withLatestSnapshots(false)

lazy val username = "openlaw"
lazy val repo = "scuttlebutt-akka-persistence"

lazy val publishSettings = Seq(
  publishArtifact in (Test, packageBin) := true,
  homepage := Some(url(s"https://github.com/$username/$repo")),
  licenses += ("Apache-2.0", url("https://opensource.org/licenses/Apache-2.0")),
  bintrayReleaseOnPublish in ThisBuild := true,
  bintrayOrganization := Some("openlawos"),
  bintrayRepository := "scuttlebutt-akka-persistence",
  bintrayPackageLabels := Seq("scuttlebutt-akka-persistence"),
  scmInfo := Some(ScmInfo(url(s"https://github.com/$username/$repo"), s"git@github.com:$username/$repo.git")),
  releaseCrossBuild := true,
  developers := List(
    Developer(
      id = "happy0",
      name = "Gordon Martin",
      email = "gordonhughmartin@gmail.com",
      url = new URL(s"http://github.com/happy0")
    ),
    Developer(
      id = "adridadou",
      name = "David Roon",
      email = "david.roon@consensys.net",
      url = new URL(s"http://github.com/adridadou")
    )
  ),
  publishTo in ThisBuild := Some("Bintray" at "https://api.bintray.com/maven/openlawos/scuttlebutt-akka-persistence/scuttlebutt-akka-persistence/;publish=1"),
)



val root = (project in file(".")).settings(publishSettings: _*)


