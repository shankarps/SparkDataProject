name := "Data Analytics Project"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

addCommandAlias("c1", "run-main climate.ClimateAnalyticsRain")
addCommandAlias("c2", "run-main climate.ClimateTemperatureAvg")
addCommandAlias("c3", "run-main climate.ClimateTemperatureMax")


outputStrategy := Some(StdoutOutput)
//outputStrategy := Some(LoggedOutput(log: Logger))

fork in run := true