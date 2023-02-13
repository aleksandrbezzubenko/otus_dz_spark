name := "SparkML_Homework"
ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "3.0.2"

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-mllib_2.12" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming_2.12" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

assembly / assemblyMergeStrategy := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
  case _ => MergeStrategy.first
}