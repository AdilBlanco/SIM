name := "SIM"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "org.apache.spark" %% "spark-hive" % "2.0.0",
  "commons-io" % "commons-io" % "2.4"
)
