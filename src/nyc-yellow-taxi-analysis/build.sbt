name := "nyc-yellow-taxi-analysis"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.10"

val sparkVersion = "3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)