name := "scala-2"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % Provided,
  "org.apache.spark" %% "spark-sql" % "1.6.0" % Provided,
  "com.databricks" %% "spark-csv" % "1.4.0"
)