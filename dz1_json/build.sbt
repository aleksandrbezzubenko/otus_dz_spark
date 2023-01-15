ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "dz1_json"
  )

val circeVersion = "0.12.0-M3"
libraryDependencies ++= Seq(
  "io.spray" %% "spray-json" % "1.3.2"
)

assemblyJarName in assembly := artifact.value.name + "-" + version.value + ".jar"
